import asyncio
import json
import os

import pytest

from app.models import Download
from app.services import pipeline as pl
from app.services.pipeline import Pipeline, classify_history_status, enqueue, queue_positions

AUDIBLE = [{
    "title": "The Vatican Conspiracy", "author": "Rob Jones", "narrator": "Colin Mace",
    "series": [{"series": "The Hunter Files", "sequence": "6"}], "asin": "B07XYZ",
    "publishedYear": "2019", "cover": "", "description": "Desc", "genres": ["Thriller"],
}]


def run(coro):
    return asyncio.run(coro)


class FakeNzbget:
    history, queue = [], []

    def __init__(self, *a, **k):
        pass

    def get_history(self):
        return FakeNzbget.history

    def get_queue(self):
        return FakeNzbget.queue


class FakeAbs:
    books = AUDIBLE
    library_items = []
    calls = []

    def __init__(self, url, token):
        self.url = url

    def search_books(self, title, author="", provider="audible"):
        FakeAbs.calls.append(("search_books", title, author, provider))
        return FakeAbs.books

    def notify_new_path(self, library_id, path):
        FakeAbs.calls.append(("watcher", path))

    def scan_library(self, library_id):
        FakeAbs.calls.append(("scan",))

    def search_library(self, library_id, query):
        return FakeAbs.library_items

    def quick_match(self, item_id, title="", author="", provider="audible", asin=""):
        FakeAbs.calls.append(("match", item_id, asin))
        return {"updated": True}

    def item_web_url(self, item_id):
        return f"{self.url}/item/{item_id}"


@pytest.fixture(autouse=True)
def fakes(monkeypatch):
    FakeNzbget.history, FakeNzbget.queue = [], []
    FakeAbs.books, FakeAbs.library_items, FakeAbs.calls = AUDIBLE, [], []
    monkeypatch.setattr(pl, "NzbgetClient", FakeNzbget)
    monkeypatch.setattr(pl, "AbsClient", FakeAbs)


def make_download(db, **fields):
    dl = Download(status=fields.pop("status", "sent"), **fields)
    db.add(dl)
    db.commit()
    return dl.id


def reload(db, dl_id):
    db.expire_all()
    return db.get(Download, dl_id)


def test_classify_history_status():
    assert classify_history_status("SUCCESS/ALL") == ("downloaded", "")
    assert classify_history_status("WARNING/SCRIPT")[0] == "downloaded"
    outcome, message = classify_history_status("WARNING/PASSWORD")
    assert outcome == "failed" and "password" in message.lower()
    assert classify_history_status("DELETED/MANUAL")[0] == "failed"
    assert classify_history_status("FAILURE/SOMETHINGNEW") == ("failed", "NZBGet finished with status FAILURE/SOMETHINGNEW")
    assert classify_history_status("") == ("", "")


def test_poll_marks_success_maps_path_and_auto_converts(db, settings, tmp_path, clean_db):
    book_dir = tmp_path / "downloads" / "The Vatican Conspiracy"
    book_dir.mkdir(parents=True)
    (book_dir / "01.mp3").write_bytes(b"x")
    settings(nzbget_url="http://nzbget", nzbget_path_prefix="/nzbget/dl", local_path_prefix=str(tmp_path / "downloads"),
             abs_url="http://abs", abs_token="t", abs_library_id="lib", ha_webhook_url="http://ha/hook")
    dl_id = make_download(db, nzbget_id=7, post_title="Book Club - Rob Jones - Hunter06 - The Vatican Conspiracy (Colin Mace)",
                          download_metadata=json.dumps({"narrator": "Forum Narrator"}))
    FakeNzbget.history = [{"NZBID": 7, "Status": "SUCCESS/ALL", "FinalDir": "", "DestDir": "/nzbget/dl/The Vatican Conspiracy"}]

    p = Pipeline()
    run(p.poll_once())

    dl = reload(db, dl_id)
    assert dl.status == "downloaded"
    assert dl.download_path.replace("\\", "/").endswith("downloads/The Vatican Conspiracy")
    meta = dl.parsed_metadata
    assert meta["asin"] == "B07XYZ" and meta["series_part"] == "6" and meta["narrator"] == "Colin Mace"
    assert dl.m4b_status == "queued" and dl.queued_at is not None
    events = [e["event"] for e in clean_db]
    assert "download_completed" in events


def test_poll_low_confidence_needs_review(db, settings, clean_db):
    settings(nzbget_url="http://nzbget", abs_url="http://abs", abs_token="t", ha_webhook_url="http://ha/hook")
    FakeAbs.books = [dict(AUDIBLE[0], title="Something Else Entirely")]
    dl_id = make_download(db, nzbget_id=8, post_title="Rob Jones - The Vatican Conspiracy")
    FakeNzbget.history = [{"NZBID": 8, "Status": "SUCCESS/ALL", "DestDir": "/nowhere"}]
    run(Pipeline().poll_once())
    dl = reload(db, dl_id)
    assert dl.m4b_status is None
    assert dl.parsed_metadata["auto_processed"] is True
    assert any(e["event"] == "needs_review" for e in clean_db)


def test_poll_warning_password_fails_with_reason(db, settings):
    settings(nzbget_url="http://nzbget")
    dl_id = make_download(db, nzbget_id=9)
    FakeNzbget.history = [{"NZBID": 9, "Status": "WARNING/PASSWORD"}]
    run(Pipeline().poll_once())
    dl = reload(db, dl_id)
    assert dl.status == "failed" and "password" in dl.error_message.lower()


def test_poll_progress_and_missing_job(db, settings):
    settings(nzbget_url="http://nzbget")
    active = make_download(db, nzbget_id=10)
    gone = make_download(db, nzbget_id=11)
    FakeNzbget.queue = [{"NZBID": 10, "FileSizeMB": 200, "RemainingSizeMB": 50}]
    p = Pipeline()
    for _ in range(pl.MISSING_POLLS_BEFORE_FAILURE):
        run(p.poll_once())
    assert reload(db, active).download_progress == 75
    assert reload(db, active).status == "sent"
    assert reload(db, gone).status == "failed"


def test_poll_survives_nzbget_outage(db, settings, monkeypatch):
    settings(nzbget_url="http://nzbget")
    dl_id = make_download(db, nzbget_id=12)

    class Down(FakeNzbget):
        def get_history(self):
            raise ConnectionError("refused")
    monkeypatch.setattr(pl, "NzbgetClient", Down)
    p = Pipeline()
    run(p.poll_once())
    assert reload(db, dl_id).status == "sent"
    assert "refused" in p.last_poll_error
    assert (reload(db, dl_id).missing_polls or 0) == 0


def _converted_setup(db, settings, tmp_path, **meta_extra):
    downloads = tmp_path / "downloads"
    src = downloads / "Book"
    src.mkdir(parents=True)
    (src / "01.mp3").write_bytes(b"x")
    (src / "02.mp3").write_bytes(b"x")
    (src / "cover.jpg").write_bytes(b"jpg")
    library_root = tmp_path / "library"
    settings(m4b_output_path=str(tmp_path / "m4b"), local_path_prefix=str(downloads),
             m4b_move_template=str(library_root).replace("\\", "/") + "/[Author]/[Series]/[BookNumber] - [Title]")
    meta = {"title": "The Vatican Conspiracy", "author": "Rob Jones", "narrator": "Colin Mace",
            "series": "The Hunter Files", "series_part": "6", "asin": "B07XYZ", "input_path": str(src), **meta_extra}
    dl_id = make_download(db, status="downloaded", download_path=str(src), download_metadata=json.dumps(meta))
    dl = db.get(Download, dl_id)
    enqueue(dl)
    db.commit()
    return dl_id, src, library_root


def fake_run_success(commands):
    async def _run(self, dl_id, cmd):
        commands.append(cmd)
        out = next(a.split("=", 1)[1] for a in cmd if a.startswith("--output-file="))
        with open(out, "wb") as f:
            f.write(b"m4b-data")
        return 0, "merging 100%"
    return _run


def test_convert_success_files_book_and_deletes_source(db, settings, tmp_path, monkeypatch):
    dl_id, src, library_root = _converted_setup(db, settings, tmp_path)
    commands = []
    monkeypatch.setattr(Pipeline, "_run", fake_run_success(commands))
    run(Pipeline().convert(dl_id))

    dl = reload(db, dl_id)
    assert dl.m4b_status == "converted", dl.error_message
    expected = library_root / "Rob Jones" / "The Hunter Files" / "6 - The Vatican Conspiracy" / "The Vatican Conspiracy.m4b"
    assert os.path.normpath(dl.m4b_path) == os.path.normpath(str(expected))
    assert expected.read_bytes() == b"m4b-data"
    metadata = json.loads((expected.parent / "metadata.json").read_text(encoding="utf-8"))
    assert metadata["series"] == ["The Hunter Files #6"] and metadata["narrators"] == ["Colin Mace"]
    assert (expected.parent / "cover.jpg").exists()
    assert not src.exists(), "source folder should be deleted after a successful conversion"
    assert dl.abs_status == "skipped"  # ABS not configured
    cmd = commands[0]
    assert cmd[:3] == ["m4b-tool", "merge", str(src)]
    assert "--writer=Colin Mace" in cmd and "--series-part=6" in cmd
    assert any(a.startswith("--cover=") for a in cmd)
    assert not (tmp_path / "m4b" / ".bookclub-work" / str(dl_id)).exists()


def test_convert_never_overwrites_existing_book(db, settings, tmp_path, monkeypatch):
    dl_id, src, library_root = _converted_setup(db, settings, tmp_path)
    existing = library_root / "Rob Jones" / "The Hunter Files" / "6 - The Vatican Conspiracy" / "The Vatican Conspiracy.m4b"
    existing.parent.mkdir(parents=True)
    existing.write_bytes(b"original")
    monkeypatch.setattr(Pipeline, "_run", fake_run_success([]))
    run(Pipeline().convert(dl_id))
    dl = reload(db, dl_id)
    assert dl.m4b_status == "m4b_failed" and "already exists" in dl.error_message
    assert existing.read_bytes() == b"original"
    assert src.exists()


def test_convert_keeps_source_outside_download_roots(db, settings, tmp_path, monkeypatch):
    dl_id, src, _ = _converted_setup(db, settings, tmp_path)
    settings(local_path_prefix=str(tmp_path / "elsewhere"), nzbget_path_prefix="")
    monkeypatch.setattr(Pipeline, "_run", fake_run_success([]))
    run(Pipeline().convert(dl_id))
    assert reload(db, dl_id).m4b_status == "converted"
    assert src.exists()


def test_convert_never_deletes_a_parent_folder_typed_into_the_form(db, settings, tmp_path, monkeypatch):
    dl_id, src, _ = _converted_setup(db, settings, tmp_path)
    sibling = src.parent / "Another Book"
    sibling.mkdir()
    (sibling / "keep.mp3").write_bytes(b"x")
    dl = db.get(Download, dl_id)
    meta = dl.parsed_metadata
    meta["input_path"] = str(src.parent)  # the whole downloads folder, not this book's folder
    dl.set_metadata(meta)
    db.commit()
    monkeypatch.setattr(Pipeline, "_run", fake_run_success([]))
    run(Pipeline().convert(dl_id))
    assert reload(db, dl_id).m4b_status == "converted"
    assert sibling.exists() and src.exists()


def test_convert_failure_keeps_source(db, settings, tmp_path, monkeypatch):
    dl_id, src, _ = _converted_setup(db, settings, tmp_path)

    async def failing(self, dl_id, cmd):
        return 1, "boom"
    monkeypatch.setattr(Pipeline, "_run", failing)
    run(Pipeline().convert(dl_id))
    dl = reload(db, dl_id)
    assert dl.m4b_status == "m4b_failed" and "code 1" in dl.error_message
    assert src.exists()


def test_single_m4b_is_copied_and_tagged_with_tone(db, settings, tmp_path, monkeypatch):
    dl_id, src, library_root = _converted_setup(db, settings, tmp_path)
    for f in src.iterdir():
        f.unlink()
    (src / "book.m4b").write_bytes(b"already-m4b")
    commands = []

    async def tone(self, dl_id, cmd):
        commands.append(cmd)
        return 0, "tagged"
    monkeypatch.setattr(Pipeline, "_run", tone)
    run(Pipeline().convert(dl_id))
    dl = reload(db, dl_id)
    assert dl.m4b_status == "converted", dl.error_message
    assert commands[0][:2] == ["tone", "tag"] and "--meta-narrator=Colin Mace" in commands[0]
    assert open(dl.m4b_path, "rb").read() == b"already-m4b"


def test_dequeued_job_is_not_converted(db, settings, tmp_path, monkeypatch):
    dl_id, _, _ = _converted_setup(db, settings, tmp_path)
    dl = db.get(Download, dl_id)
    dl.m4b_status = None
    db.commit()
    called = []
    monkeypatch.setattr(Pipeline, "_run", fake_run_success(called))
    run(Pipeline().convert(dl_id))
    assert not called and reload(db, dl_id).m4b_status is None


def test_queue_order_and_recovery(db):
    a = make_download(db, status="downloaded", m4b_status="converting")
    b = make_download(db, status="downloaded")
    dl = db.get(Download, b)
    enqueue(dl)
    db.commit()
    Pipeline().recover()
    positions = queue_positions(db)
    assert set(positions) == {a, b}
    assert reload(db, a).m4b_status == "queued"


def test_abs_import_matches_by_path_not_title(db, settings, tmp_path, clean_db):
    settings(abs_url="http://abs", abs_token="t", abs_library_id="lib", ha_webhook_url="http://ha/hook",
             abs_path_local="/Media/Audiobooks", abs_path_remote="/audiobooks")
    m4b = "/Media/Audiobooks/Rob Jones/The Hunter Files/6 - The Vatican Conspiracy/The Vatican Conspiracy.m4b"
    dl_id = make_download(db, status="downloaded", m4b_status="converted", m4b_path=m4b,
                          download_metadata=json.dumps({"title": "The Vatican Conspiracy", "author": "Rob Jones",
                                                        "asin": "B07XYZ"}))
    FakeAbs.library_items = [
        {"id": "old-copy", "path": "/audiobooks/Rob Jones/The Vatican Conspiracy"},
        {"id": "new", "path": "/audiobooks/Rob Jones/The Hunter Files/6 - The Vatican Conspiracy"},
    ]
    run(Pipeline().abs_import(dl_id, poll_seconds=0, attempts=2))
    dl = reload(db, dl_id)
    assert dl.abs_status == "imported" and dl.abs_item_id == "new"
    assert ("watcher", m4b.replace("/Media/Audiobooks", "/audiobooks")) in FakeAbs.calls
    assert ("match", "new", "B07XYZ") in FakeAbs.calls
    imported = [e for e in clean_db if e["event"] == "book_imported"]
    assert imported and imported[0]["url"] == "http://abs/item/new"


def test_abs_import_not_found(db, settings):
    settings(abs_url="http://abs", abs_token="t", abs_library_id="lib")
    dl_id = make_download(db, status="downloaded", m4b_status="converted", m4b_path="/lib/A/T/T.m4b",
                          download_metadata=json.dumps({"title": "T"}))
    FakeAbs.library_items = [{"id": "x", "path": "/somewhere/else"}]
    run(Pipeline().abs_import(dl_id, poll_seconds=0, attempts=5))
    dl = reload(db, dl_id)
    assert dl.abs_status == "not_found" and "path mapping" in dl.error_message
    assert ("scan",) in FakeAbs.calls  # fell back to a full library scan


def test_upgrade_marks_old_downloads_as_handled(db, settings):
    old = make_download(db, status="downloaded", download_metadata=json.dumps({"title": "Old"}))
    Pipeline().recover()
    assert reload(db, old).parsed_metadata["auto_processed"] is True
    # Only once: later downloads are auto-processed normally
    new = make_download(db, status="downloaded")
    Pipeline().recover()
    assert "auto_processed" not in reload(db, new).parsed_metadata


def test_stop_before_the_tool_starts(db, settings, tmp_path, monkeypatch):
    dl_id, src, _ = _converted_setup(db, settings, tmp_path)
    p = Pipeline()
    calls = []
    original_find = pl.library.find_audio_files

    def find_and_stop(path):
        p.cancel(dl_id)  # user presses Stop while the folder is being scanned
        return original_find(path)
    monkeypatch.setattr(pl.library, "find_audio_files", find_and_stop)
    monkeypatch.setattr(Pipeline, "_run", fake_run_success(calls))
    run(p.convert(dl_id))
    dl = reload(db, dl_id)
    assert not calls and dl.m4b_status is None and dl.error_message == "Conversion stopped"
    assert src.exists()


def test_late_stop_does_not_poison_the_next_run(db, settings, tmp_path, monkeypatch):
    dl_id, _, _ = _converted_setup(db, settings, tmp_path)
    p = Pipeline()
    monkeypatch.setattr(Pipeline, "_run", fake_run_success([]))
    run(p.convert(dl_id))
    assert p.cancel(dl_id) is False  # finished — nothing to stop
    assert dl_id not in p.cancelled


def test_worker_survives_errors(db, monkeypatch):
    p = Pipeline()
    calls = {"n": 0}

    def flaky():
        calls["n"] += 1
        if calls["n"] == 1:
            raise RuntimeError("database is locked")
        raise asyncio.CancelledError()
    monkeypatch.setattr(p, "_next_queued", flaky)
    real_sleep = asyncio.sleep
    monkeypatch.setattr(pl.asyncio, "sleep", lambda s: real_sleep(0))
    with pytest.raises(asyncio.CancelledError):
        run(p._worker_loop())
    assert calls["n"] == 2
