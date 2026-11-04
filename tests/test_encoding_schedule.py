"""Encoding hours, the CPU temperature limit, and Audnexus chapters."""
import asyncio
import json
import signal
from datetime import time

import pytest

from app.models import Download
from app.services import audio, chapters
from app.services import pipeline as pl


# ── hours and temperature ─────────────────────────────────────────────────────

def test_window_including_overnight():
    assert audio.in_window(time(2, 0), "01:00", "07:00")
    assert not audio.in_window(time(8, 0), "01:00", "07:00")
    assert audio.in_window(time(23, 30), "22:00", "06:00") and audio.in_window(time(5, 0), "22:00", "06:00")
    assert not audio.in_window(time(12, 0), "22:00", "06:00")
    assert audio.in_window(time(12, 0), "", "") and audio.in_window(time(12, 0), "01:00", "")


def test_encode_gate():
    hours = {"encode_window_start": "01:00", "encode_window_end": "07:00"}
    assert "encoding hours (01:00–07:00)" in audio.encode_gate(hours, now_time=time(12, 0))
    assert audio.encode_gate(hours, now_time=time(3, 0)) == ""
    hot = {"encode_max_temp": "80"}
    assert "CPU is 85°C" in audio.encode_gate(hot, temperature=85.0, now_time=time(3, 0))
    assert audio.encode_gate(hot, temperature=70.0, now_time=time(3, 0)) == ""
    assert not audio.restrictions_set({}) and audio.restrictions_set(hot) and audio.restrictions_set(hours)


def test_cpu_temperature_prefers_cpu_sensors(tmp_path):
    def sensor(folder, name, *millidegrees):
        path = tmp_path / "hwmon" / folder
        path.mkdir(parents=True)
        (path / "name").write_text(name)
        for i, value in enumerate(millidegrees, 1):
            (path / f"temp{i}_input").write_text(str(value))
    sensor("hwmon0", "acpitz", 99000)
    sensor("hwmon1", "nvme", 50000)
    sensor("hwmon2", "coretemp", 61000, 67500)
    assert audio.cpu_temperature(str(tmp_path)) == 67.5
    assert audio.cpu_temperature(str(tmp_path / "missing")) is None


def test_local_time_zone():
    assert audio.local_now("Australia/Brisbane") is not None
    assert audio.local_now("Not/AZone") is not None  # falls back to the container clock


def _queued(db, tmp_path, name, files, queued_at):
    folder = tmp_path / name
    folder.mkdir()
    for f in files:
        (folder / f).write_bytes(b"x")
    dl = Download(status="downloaded", m4b_status="queued", download_path=str(folder), queued_at=queued_at,
                  download_metadata=json.dumps({"input_path": str(folder)}))
    db.add(dl)
    db.commit()
    return dl.id


def test_outside_the_hours_only_quick_books_go_ahead(db, settings, tmp_path, monkeypatch):
    from datetime import datetime, timedelta
    now = datetime.utcnow()
    mp3_book = _queued(db, tmp_path, "mp3", ["1.mp3", "2.mp3"], now - timedelta(minutes=5))
    m4b_book = _queued(db, tmp_path, "m4b", ["book.m4b"], now)
    monkeypatch.setattr(pl.audio, "probe_all", lambda files: [{"codec": "mp3"} for _ in files])
    p = pl.Pipeline()
    assert p._next_queued() == mp3_book  # no limits: queue order

    settings(encode_window_start="01:00", encode_window_end="01:01")
    monkeypatch.setattr(pl.audio, "local_now", lambda tz: datetime(2026, 1, 1, 12, 0))
    assert p._next_queued() == m4b_book  # the MP3 book waits; the copy-only one doesn't
    assert "encoding hours" in p.encode_wait
    db.get(Download, m4b_book).m4b_status = "converted"
    db.commit()
    assert p._next_queued() is None
    db.expire_all()
    assert db.get(Download, mp3_book).parsed_metadata["encode_check"]["needs"] is True

    # The page says why it's waiting
    from fastapi.testclient import TestClient
    from app.main import app
    monkeypatch.setattr(pl.pipeline, "encode_wait", p.encode_wait)
    page = TestClient(app).post("/downloads/sync-status", data={"dl_filter": "all"}).text
    assert "Waiting for the encoding hours" in page


@pytest.mark.skipif(not hasattr(signal, "SIGSTOP"), reason="pausing uses SIGSTOP (Linux, the container)")
def test_hot_cpu_pauses_and_resumes_the_encoder(monkeypatch):
    sent = []
    monkeypatch.setattr(pl.os, "killpg", lambda pid, sig: sent.append(sig))
    monkeypatch.setattr(pl, "TEMPERATURE_CHECK_SECONDS", 0)
    readings = iter([85.0, 82.0, 74.0, 74.0])
    monkeypatch.setattr(pl.audio, "cpu_temperature", lambda: next(readings, 74.0))

    class Proc:
        pid, returncode = 4242, None

    async def scenario():
        p = pl.Pipeline()
        p.current_proc, p.current_id = Proc(), 1
        task = asyncio.create_task(p._watch_temperature(80))
        for _ in range(500):  # until it has paused and resumed (each reading runs in a thread)
            if len(sent) >= 2:
                break
            await asyncio.sleep(0.01)
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)
        return p
    p = asyncio.run(scenario())
    assert sent[:2] == [signal.SIGSTOP, signal.SIGCONT] and p.encode_note == ""


# ── chapters ──────────────────────────────────────────────────────────────────

AUDNEXUS = {"runtimeLengthMs": 3_600_000, "chapters": [
    {"startOffsetMs": 0, "title": "Opening Credits"},
    {"startOffsetMs": 15_000, "title": "Prologue"},
    {"startOffsetMs": 1_234_567, "title": "Chapter One:  The   Beginning"},
]}


def test_generic_chapters_are_recognised():
    assert chapters.looks_generic([])
    assert chapters.looks_generic([{"title": "Chapter 1"}, {"title": "Chapter 2"}])
    assert chapters.looks_generic([{"title": "Project Hail Mary - 01"}, {"title": "Project Hail Mary - 02"}])
    assert chapters.looks_generic([{"title": "001"}, {"title": "002"}])
    assert not chapters.looks_generic([{"title": "Prologue"}, {"title": "The Beginning"}])


def test_chapter_file_format_and_runtime_check():
    text = chapters.chapters_file_text(AUDNEXUS)
    assert text.splitlines() == ["00:00:00.000 Opening Credits", "00:00:15.000 Prologue",
                                 "00:20:34.567 Chapter One: The Beginning"]
    assert chapters.runtime_matches(AUDNEXUS, 3_630)
    assert not chapters.runtime_matches(AUDNEXUS, 3_000)


def test_apply_writes_chapters_only_when_it_fits(tmp_path, monkeypatch):
    calls = []

    class Done:
        returncode, stdout, stderr = 0, "", ""

    def fake_run(cmd, **k):
        calls.append(cmd)
        return Done()
    monkeypatch.setattr(chapters, "fetch", lambda asin, provider: AUDNEXUS)
    monkeypatch.setattr(chapters, "read_chapters", lambda path: ([{"title": "Track 1"}, {"title": "Track 2"}], 3_600.0))
    assert chapters.apply("book.m4b", "B0ABCDEFGH", "audible.uk", str(tmp_path), run=fake_run) == \
        "named 3 chapters from Audible"
    assert calls[0][:3] == ["tone", "tag", "book.m4b"] and calls[0][3].startswith("--meta-chapters-file=")
    assert (tmp_path / "audnexus-chapters.txt").read_text(encoding="utf-8").startswith("00:00:00.000 Opening Credits")

    calls.clear()
    monkeypatch.setattr(chapters, "read_chapters", lambda path: ([{"title": "Prologue"}, {"title": "Dawn"}], 3_600.0))
    assert "kept the book's own 2 named chapters" in chapters.apply("b", "B0ABCDEFGH", "audible", str(tmp_path), run=fake_run)
    monkeypatch.setattr(chapters, "read_chapters", lambda path: ([], 1_800.0))
    assert "kept the existing chapters" in chapters.apply("b", "B0ABCDEFGH", "audible", str(tmp_path), run=fake_run)
    assert calls == []


def test_fetch_rejects_a_bad_asin(monkeypatch):
    monkeypatch.setattr(chapters.requests, "get", lambda *a, **k: pytest.fail("should not ask"))
    assert chapters.fetch("not an asin") is None
