"""Database migrations, live updates (events + /events stream), the background log writer, covers."""
import asyncio
import json
import os

from sqlalchemy import create_engine, text

from app import database, events, log_handler
from app.models import Download, LogEntry
from app.routers import live


# ── migrations ────────────────────────────────────────────────────────────────

def test_migration_moves_auto_state_into_columns(tmp_path, monkeypatch):
    """An old database: the automatic match's state lived in the JSON metadata."""
    engine = create_engine(f"sqlite:///{(tmp_path / 'old.db').as_posix()}")
    with engine.begin() as conn:
        conn.execute(text("CREATE TABLE downloads (id INTEGER PRIMARY KEY, status TEXT, search_term TEXT, "
                          "nzb_hash TEXT, created_at DATETIME, download_metadata TEXT)"))
        conn.execute(text("CREATE TABLE book_requests (id INTEGER PRIMARY KEY, status TEXT)"))
        conn.execute(text("CREATE TABLE logs (id INTEGER PRIMARY KEY, download_id INTEGER)"))
        conn.execute(text("INSERT INTO downloads (id, status, download_metadata) VALUES (1, 'downloaded', :m)"),
                     {"m": json.dumps({"title": "Kept", "auto_processed": True, "auto_attempts": 2,
                                       "auto_next_try": "2026-01-02T03:04:05", "review_reason": "Pick one"})})
        conn.execute(text("INSERT INTO downloads (id, status, download_metadata) VALUES (2, 'sent', '{\"title\": \"New\"}')"))
    monkeypatch.setattr(database, "engine", engine)
    database.migrate_db()
    database.migrate_db()  # a second start changes nothing

    with engine.connect() as conn:
        assert conn.execute(text("PRAGMA user_version")).scalar() == len(database.MIGRATIONS)
        row = conn.execute(text("SELECT auto_processed, auto_attempts, auto_next_try, review_reason, download_metadata "
                                "FROM downloads WHERE id = 1")).one()
        assert row[0] == 1 and row[1] == 2 and row[2].startswith("2026-01-02 03:04:05") and row[3] == "Pick one"
        assert json.loads(row[4]) == {"title": "Kept"}
        assert conn.execute(text("SELECT auto_processed, auto_attempts FROM downloads WHERE id = 2")).one() == (0, 0)
        indexes = {r[1] for r in conn.execute(text("PRAGMA index_list(downloads)")).all()}
        assert {"ix_downloads_status", "ix_downloads_m4b_status", "ix_downloads_auto_processed"} <= indexes


def test_new_databases_have_the_indexes():
    with database.engine.connect() as conn:
        indexes = {r[1] for r in conn.execute(text("PRAGMA index_list(downloads)")).all()}
        log_indexes = {r[1] for r in conn.execute(text("PRAGMA index_list(logs)")).all()}
    assert {name for name, table, _ in database.INDEXES if table == "downloads"} <= indexes
    assert "ix_logs_download_id" in log_indexes


# ── live updates ──────────────────────────────────────────────────────────────

class _Collect:
    def __init__(self, monkeypatch):
        self.published = []
        monkeypatch.setattr(events.bus, "publish", lambda topics: self.published.append(set(topics)))


def test_commits_announce_what_changed(db, monkeypatch):
    seen = _Collect(monkeypatch)
    db.add(Download(status="sent", post_title="A"))
    db.commit()
    assert seen.published == [{"downloads"}]
    dl = db.query(Download).first()
    dl.download_progress = 40
    db.add(LogEntry(level="INFO", message="x"))
    db.commit()
    assert seen.published[-1] == {"downloads", "logs"}
    db.query(LogEntry).delete()  # bulk delete: no flush lists
    db.commit()
    assert seen.published[-1] == {"logs"}


def test_rolled_back_changes_are_not_announced(db, monkeypatch):
    seen = _Collect(monkeypatch)
    db.add(Download(status="sent"))
    db.flush()
    db.rollback()
    db.commit()
    assert seen.published == []


class _Req:
    async def is_disconnected(self):
        return False


def test_event_stream_sends_changes_and_ends():
    async def scenario():
        bus = events.EventBus()
        bus.attach(asyncio.get_running_loop())
        original, events.bus = events.bus, bus
        live.bus = bus
        try:
            stream = live.event_stream(_Req(), ping_seconds=0.05, max_seconds=2)
            assert (await stream.__anext__()).startswith("retry:")
            assert "event: hello" in await stream.__anext__()
            bus.publish({"downloads"})
            bus.publish({"requests"})  # coalesced into one message
            message = await stream.__anext__()
            assert message == "event: change\ndata: downloads,requests\n\n"
            assert await stream.__anext__() == ": ping\n\n"
            bus.close()
            try:
                while True:
                    await stream.__anext__()
            except StopAsyncIteration:
                pass
            assert not bus.subscribers
        finally:
            events.bus = live.bus = original
    asyncio.run(scenario())


def test_pages_listen_for_live_updates(client):
    assert 'bcDownloads from:body' in client.get("/downloads").text
    page = client.get("/m").text
    assert 'bcRequests from:body' in page and "[bcShouldPoll()]" in page
    js = client.get("/static/js/bookclub.js").text
    assert "new EventSource('/events')" in js and "navigator.locks" in js and "visibilitychange" in js


# ── log writer ────────────────────────────────────────────────────────────────

def test_log_lines_are_written_in_the_background(db, monkeypatch):
    monkeypatch.delenv("BOOKCLUB_SYNC_LOGS", raising=False)
    log_handler.log_to_db("INFO", "test", "queued line", download_id=5)
    import logging
    logging.getLogger("app.services.test").warning("through logging")
    assert log_handler.flush(5)
    db.expire_all()
    messages = {(e.message, e.download_id, e.source) for e in db.query(LogEntry).all()}
    assert ("queued line", 5, "test") in messages and ("through logging", None, "test") in messages


def test_log_writer_survives_a_bad_line(db, monkeypatch, capsys):
    monkeypatch.delenv("BOOKCLUB_SYNC_LOGS", raising=False)
    log_handler.log_to_db("INFO", "test", "before the bad one")
    log_handler._writer.put({"level": "INFO", "message": None, "source": "x"})  # NOT NULL message: write fails
    log_handler.log_to_db("INFO", "test", "after the bad one")
    assert log_handler.flush(5)
    db.expire_all()
    kept = {e.message for e in db.query(LogEntry).all()}
    assert {"before the bad one", "after the bad one"} <= kept  # same batch, still written


# ── covers ────────────────────────────────────────────────────────────────────

class _ImageResponse:
    headers = {"content-type": "image/jpeg"}

    def raise_for_status(self):
        pass

    def iter_content(self, size):
        yield b"\xff\xd8\xff" + b"0" * 2000


def test_covers_are_cached_on_disk(client, db, monkeypatch):
    from app.services import covers
    calls = []
    monkeypatch.setattr(covers.requests, "get", lambda url, **k: calls.append(url) or _ImageResponse())
    dl = Download(status="downloaded", download_metadata=json.dumps({"cover_url": "https://img.example/c.jpg"}))
    db.add(dl)
    db.commit()
    for _ in range(2):
        r = client.get(f"/covers/{dl.id}")
        assert r.status_code == 200 and r.headers["content-type"] == "image/jpeg"
        assert "max-age" in r.headers["cache-control"]
    assert calls == ["https://img.example/c.jpg"]  # fetched once
    assert os.path.isfile(covers.cached_path("https://img.example/c.jpg"))
    assert client.get("/covers/999999").status_code == 404


def test_cover_that_cannot_be_fetched_falls_back_to_the_original(client, db, monkeypatch):
    from app.services import covers

    def fail(url, **k):
        raise OSError("offline")
    monkeypatch.setattr(covers.requests, "get", fail)
    dl = Download(status="downloaded", download_metadata=json.dumps({"cover_url": "https://img.example/x.jpg"}))
    db.add(dl)
    db.commit()
    r = client.get(f"/covers/{dl.id}", follow_redirects=False)
    assert r.status_code == 302 and r.headers["location"] == "https://img.example/x.jpg"


def test_phone_cards_use_the_cached_cover(client, db):
    db.add(Download(status="downloaded", post_title="Has Cover",
                    download_metadata=json.dumps({"title": "Has Cover", "cover_url": "https://img.example/c.jpg"})))
    db.commit()
    page = client.post("/downloads/sync-status", data={"dl_filter": "all"}, headers={"X-BC-UI": "mobile"}).text
    assert 'src="/covers/' in page and "img.example" not in page
