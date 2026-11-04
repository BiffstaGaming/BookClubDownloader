"""Review reasons, retries, the Claude reviewer in the pipeline, and encoding choices."""
import json

from app.services import ai
from app.services import pipeline as pl
from app.services.pipeline import Pipeline

from test_pipeline import (  # noqa: F401 — `fakes` is an autouse fixture
    AUDIBLE, FakeAbs, _converted_setup, fake_run_success, fakes, make_download, reload, run,
)


def _book_folder(tmp_path, name="dl"):
    src = tmp_path / name
    src.mkdir()
    (src / "a.mp3").write_bytes(b"x")
    return src


def test_audible_outage_is_retried_not_sent_for_review(db, settings, tmp_path, clean_db, monkeypatch):
    src = _book_folder(tmp_path)
    settings(abs_url="http://abs", abs_token="t", ha_webhook_url="http://ha/hook")

    def down(self, *a):
        raise ConnectionError("ABS restarting")
    monkeypatch.setattr(FakeAbs, "search_books", down)
    dl_id = make_download(db, status="downloaded", download_path=str(src), post_title="Rob Jones - The Vatican Conspiracy")
    run(Pipeline().auto_process(dl_id))
    dl = reload(db, dl_id)
    assert not dl.auto_processed and dl.auto_attempts == 1 and dl.auto_next_try
    assert "trying again" in dl.review_reason
    assert not any(e["event"] == "needs_review" for e in clean_db)

    # Not due yet: the poller leaves it alone.
    run(Pipeline().poll_once())
    assert reload(db, dl_id).auto_attempts == 1

    # Out of retries: a person is asked.
    dl = reload(db, dl_id)
    dl.auto_attempts = len(pl.AUTO_RETRY_MINUTES)
    dl.auto_next_try = None
    db.commit()
    run(Pipeline().poll_once())
    dl = reload(db, dl_id)
    assert dl.auto_processed is True and "Audible search failed" in dl.review_reason
    assert any(e["event"] == "needs_review" for e in clean_db)


def test_series_in_the_author_slot_is_not_searched_as_the_author(db, settings, tmp_path):
    src = _book_folder(tmp_path)
    settings(abs_url="http://abs", abs_token="t")
    dl_id = make_download(db, status="downloaded", download_path=str(src),
                          post_title="Rob Jones - Hunter Files 06 - The Vatican Conspiracy")
    run(Pipeline().auto_process(dl_id))
    searches = [c for c in FakeAbs.calls if c[0] == "search_books"]
    assert searches[0][1:3] == ("The Vatican Conspiracy", "")
    assert reload(db, dl_id).m4b_status == "queued"


def test_subtitled_audible_title_converts_automatically(db, settings, tmp_path):
    src = _book_folder(tmp_path)
    settings(abs_url="http://abs", abs_token="t")
    FakeAbs.books = [dict(AUDIBLE[0], title="The Vatican Conspiracy: A Hunter Files Thriller")]
    dl_id = make_download(db, status="downloaded", download_path=str(src),
                          post_title="Book Club - Rob Jones - Hunter06 - The Vatican Conspiracy (Colin Mace)")
    run(Pipeline().auto_process(dl_id))
    dl = reload(db, dl_id)
    assert dl.m4b_status == "queued" and dl.parsed_metadata["match_method"].startswith("rules")


def _claude(monkeypatch, settings, reply):
    settings(ai_enabled="1", claude_oauth_token="tok")
    monkeypatch.setattr(ai, "cli_path", lambda: "claude")
    prompts = []

    def fake(prompt, model, token):
        prompts.append(prompt)
        return ai.ClaudeResult(text=reply)
    monkeypatch.setattr(ai, "_call", fake)
    return prompts


def test_claude_settles_an_uncertain_match(db, settings, tmp_path, monkeypatch):
    src = _book_folder(tmp_path)
    settings(abs_url="http://abs", abs_token="t")
    FakeAbs.books = [dict(AUDIBLE[0], title="Conspiracy at the Vatican", asin="RIGHT")]
    prompts = _claude(monkeypatch, settings, '{"choice": 1, "confidence": 96, "reason": "same book and narrator"}')
    dl_id = make_download(db, status="downloaded", download_path=str(src),
                          post_title="Book Club - Rob Jones - Hunter06 - The Vatican Conspiracy (Colin Mace)")
    run(Pipeline().auto_process(dl_id))
    dl = reload(db, dl_id)
    assert prompts and "Conspiracy at the Vatican" in prompts[0]
    assert dl.m4b_status == "queued" and dl.parsed_metadata["asin"] == "RIGHT"
    assert "Claude" in dl.parsed_metadata["match_method"]


def test_claude_cannot_override_the_language_check(db, settings, tmp_path, monkeypatch):
    src = _book_folder(tmp_path)
    settings(abs_url="http://abs", abs_token="t")
    FakeAbs.books = [dict(AUDIBLE[0], title="The Poison Daughter", author="Sheila Masterson", language="german")]
    _claude(monkeypatch, settings, '{"choice": 1, "confidence": 99, "reason": "same title"}')
    dl_id = make_download(db, status="downloaded", download_path=str(src),
                          post_title="Book Club - Sheila Masterson - The Poison Daughter (2026)")
    run(Pipeline().auto_process(dl_id))
    dl = reload(db, dl_id)
    assert dl.m4b_status is None and "not in English" in dl.review_reason


def test_unsure_claude_leaves_it_for_review(db, settings, tmp_path, monkeypatch):
    src = _book_folder(tmp_path)
    settings(abs_url="http://abs", abs_token="t")
    FakeAbs.books = [dict(AUDIBLE[0], title="Something Else Entirely")]
    _claude(monkeypatch, settings, '{"choice": null, "confidence": 0, "reason": "none of these is the book"}')
    dl_id = make_download(db, status="downloaded", download_path=str(src), post_title="Rob Jones - The Vatican Conspiracy")
    run(Pipeline().auto_process(dl_id))
    dl = reload(db, dl_id)
    assert dl.m4b_status is None and "none of these is the book" in dl.review_reason


# ── encoding ──────────────────────────────────────────────────────────────────

def test_aac_parts_are_joined_without_re_encoding(db, settings, tmp_path, monkeypatch):
    dl_id, src, _ = _converted_setup(db, settings, tmp_path)
    for f in src.glob("*.mp3"):
        f.rename(f.with_suffix(".m4a"))
    monkeypatch.setattr(pl.audio, "probe_all",
                        lambda files: [{"codec": "aac", "sample_rate": 44100, "channels": 2, "seconds": 3600}] * len(files))
    commands = []
    monkeypatch.setattr(Pipeline, "_run", fake_run_success(commands))
    run(Pipeline().convert(dl_id))
    dl = reload(db, dl_id)
    assert dl.m4b_status == "converted", dl.error_message
    assert "--no-conversion" in commands[0] and not any(a.startswith("--audio-bitrate") for a in commands[0])
    assert dl.parsed_metadata["convert_mode"] == "joined without re-encoding"


def test_mixed_formats_are_still_encoded(db, settings, tmp_path, monkeypatch):
    dl_id, src, _ = _converted_setup(db, settings, tmp_path)
    (src / "01.mp3").rename(src / "01.m4a")
    monkeypatch.setattr(pl.audio, "probe_all",
                        lambda files: [{"codec": "aac" if f.endswith(".m4a") else "mp3", "sample_rate": 44100,
                                        "channels": 2, "seconds": 60} for f in files])
    commands = []
    monkeypatch.setattr(Pipeline, "_run", fake_run_success(commands))
    run(Pipeline().convert(dl_id))
    assert "--no-conversion" not in commands[0]


def test_mp3_is_encoded_for_speech_with_the_core_limit(db, settings, tmp_path, monkeypatch):
    dl_id, src, _ = _converted_setup(db, settings, tmp_path)
    settings(m4b_jobs="8", encode_max_cores="2")
    monkeypatch.setattr(pl.audio, "available_cores", lambda: 16)
    commands = []
    monkeypatch.setattr(Pipeline, "_run", fake_run_success(commands))
    run(Pipeline().convert(dl_id))
    cmd = commands[0]
    assert "--no-conversion" not in cmd
    for flag in ("--audio-bitrate=64k", "--audio-channels=1", "--audio-samplerate=22050", "--jobs=2", "--ffmpeg-threads=2"):
        assert flag in cmd, flag


def test_gentle_wraps_with_nice_and_taskset(monkeypatch):
    monkeypatch.setattr(pl.audio.os, "name", "posix")
    monkeypatch.setattr(pl.audio.shutil, "which", lambda name: f"/usr/bin/{name}")
    monkeypatch.setattr(pl.audio, "allowed_cpus", lambda: [4, 5, 6, 7])  # container pinned to cores 4-7
    assert pl.audio.gentle(["m4b-tool"], 3) == ["nice", "-n", "10", "taskset", "-c", "4,5,6", "m4b-tool"]
    assert pl.audio.gentle(["m4b-tool"], 0) == ["nice", "-n", "10", "m4b-tool"]


def test_speed_description():
    assert pl.audio.describe_speed(41520, 190) == "11h 32m of audio in 3m 10s (219× real time)"


def test_imported_at_and_rich_notification(db, settings, tmp_path, clean_db):
    settings(abs_url="http://abs", abs_token="t", abs_library_id="lib", ha_webhook_url="http://ha/hook",
             public_url="http://bookclub:9933")
    book_dir = tmp_path / "lib" / "Rob Jones" / "The Vatican Conspiracy"
    book_dir.mkdir(parents=True)
    m4b = book_dir / "The Vatican Conspiracy.m4b"
    m4b.write_bytes(b"x")
    dl_id = make_download(db, status="downloaded", m4b_status="converted", m4b_path=str(m4b), requested_by="janet",
                          download_metadata=json.dumps({"title": "The Vatican Conspiracy", "author": "Rob Jones",
                                                        "narrator": "Colin Mace", "cover_url": "http://c/x.jpg"}))
    FakeAbs.library_items = [{"id": "li_1", "path": str(book_dir).replace("\\", "/")}]
    run(Pipeline().abs_import(dl_id, poll_seconds=0, attempts=1))
    dl = reload(db, dl_id)
    assert dl.abs_status == "imported" and dl.imported_at is not None
    event = next(e for e in clean_db if e["event"] == "book_imported")
    assert event["requested_by"] == "janet" and event["narrator"] == "Colin Mace"
    assert event["cover_url"] == "http://c/x.jpg" and event["url"] == "http://abs/item/li_1"
    assert event["bookclub_url"] == f"http://bookclub:9933/downloads?highlight={dl_id}"


def test_unwanted_events_are_not_sent(db, settings, clean_db):
    from app.services import notify
    settings(ha_webhook_url="http://ha/hook", ha_events="book_imported")
    assert notify.send_event_sync("download_completed", title="x") is False
    assert notify.send_event_sync("book_imported", title="x") is True
    assert [e["event"] for e in clean_db] == ["book_imported"]
