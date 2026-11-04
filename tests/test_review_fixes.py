"""Regression tests for the bugs found in the second full review."""
import asyncio
import json

from app.models import Download, LogEntry, RevealedPost
from app.routers import settings as settings_router
from app.services import ai, book_requests as br, forum_cache, notify
from app.services import pipeline as pl
from app.services.pipeline import Pipeline
from app.settings_store import get_setting
from app.services import nzb as nzb_mod

from test_pipeline import (  # noqa: F401 — `fakes` is an autouse fixture
    AUDIBLE, FakeAbs, _converted_setup, fake_run_success, fakes, make_download, reload, run,
)


# ── settings / security ───────────────────────────────────────────────────────

def test_test_buttons_never_send_saved_secrets_to_another_address(client, settings, monkeypatch):
    settings(nzbget_url="http://nzbget:6789", nzbget_username="nzbget", nzbget_password="SuperSecret",
             abs_url="http://abs:13378", abs_token="ABS-SECRET")
    seen = []

    class Spy:
        def __init__(self, url, user, password):
            seen.append((url, password))

        def test_connection(self):
            return True
    monkeypatch.setattr(settings_router, "NzbgetClient", Spy)
    client.post("/settings/test-nzbget", data={"nzbget_url": "http://attacker.example"})
    client.post("/settings/test-nzbget", data={})                                   # saved address: fine
    client.post("/settings/test-nzbget", data={"nzbget_url": "http://nzbget:6789/"})  # same address
    assert seen == [("http://attacker.example", ""), ("http://nzbget:6789", "SuperSecret"),
                    ("http://nzbget:6789/", "SuperSecret")]

    used = []

    class AbsSpy:
        def __init__(self, url, token):
            used.append(token)

        def get_libraries(self):
            return []
    monkeypatch.setattr(settings_router, "AbsClient", AbsSpy)
    r = client.post("/settings/abs-libraries", data={"abs_url": "http://attacker.example"})
    assert used == [] and "required" in r.text


def test_cross_site_changes_are_refused(client):
    r = client.post("/downloads/requests", data={"query": "Dune"}, headers={"Sec-Fetch-Site": "cross-site"})
    assert r.status_code == 403
    assert client.post("/downloads/requests", data={"query": "Dune"},
                       headers={"Sec-Fetch-Site": "same-origin"}).status_code == 200
    assert client.get("/downloads", headers={"Sec-Fetch-Site": "cross-site"}).status_code == 200


def test_api_token_cannot_be_set_by_a_form(client, db):
    from app.routers.api import ensure_api_token
    token = ensure_api_token(db)
    client.post("/settings", data={"api_token": "attacker-known"})
    db.expire_all()
    assert get_setting(db, "api_token") == token


def test_saved_secrets_can_be_forgotten(client, db, settings):
    settings(abs_token="ABS-SECRET", nzbget_password="pw")
    client.post("/settings", data={"abs_token": "", "abs_token__clear": "1", "nzbget_password": ""})
    db.expire_all()
    assert get_setting(db, "abs_token") == "" and get_setting(db, "nzbget_password") == "pw"


def test_unicode_digits_do_not_crash(client):
    assert client.get("/logs/entries", params={"download": "²"}).status_code == 200


def test_nzbget_reachability_is_unknown_when_nothing_downloads(client):
    pl.pipeline.nzbget_reachable = None
    assert client.get("/api/status").json()["nzbget_reachable"] is None


# ── pipeline ──────────────────────────────────────────────────────────────────

def test_abridged_edition_never_auto_converts_even_with_a_low_threshold(db, settings, tmp_path, monkeypatch):
    src = tmp_path / "dl"
    src.mkdir()
    (src / "a.mp3").write_bytes(b"x")
    settings(abs_url="http://abs", abs_token="t", auto_match_threshold="60")
    monkeypatch.setattr(pl.audio, "probe_all", lambda files: [{"codec": "mp3", "seconds": 20 * 3600}])
    FakeAbs.books = [dict(AUDIBLE[0], duration=300)]  # 5 h on Audible vs 20 h downloaded
    dl_id = make_download(db, status="downloaded", download_path=str(src), post_title="Rob Jones - The Vatican Conspiracy")
    run(Pipeline().auto_process(dl_id))
    dl = reload(db, dl_id)
    assert dl.m4b_status is None and "different length" in dl.review_reason


def test_manual_choice_while_auto_match_runs_is_kept(db, settings, tmp_path, monkeypatch):
    src = tmp_path / "dl"
    src.mkdir()
    (src / "a.mp3").write_bytes(b"x")
    settings(abs_url="http://abs", abs_token="t")
    dl_id = make_download(db, status="downloaded", download_path=str(src), post_title="Rob Jones - The Vatican Conspiracy")

    def search_while_user_converts(self, title, author="", provider="audible"):
        other = pl.SessionLocal()
        row = other.get(Download, dl_id)
        pl.mark_handled_by_hand(row)
        meta = row.parsed_metadata
        meta["title"] = "Chosen By Hand"
        row.set_metadata(meta)
        pl.enqueue(row)
        other.commit()
        other.close()
        return AUDIBLE
    monkeypatch.setattr(FakeAbs, "search_books", search_while_user_converts)
    run(Pipeline().auto_process(dl_id))
    dl = reload(db, dl_id)
    assert dl.parsed_metadata["title"] == "Chosen By Hand" and dl.m4b_status == "queued"
    assert "match_method" not in dl.parsed_metadata


def test_no_audible_results_is_retried_twice_first(db, settings, tmp_path):
    src = tmp_path / "dl"
    src.mkdir()
    (src / "a.mp3").write_bytes(b"x")
    settings(abs_url="http://abs", abs_token="t")
    FakeAbs.books = []
    dl_id = make_download(db, status="downloaded", download_path=str(src), post_title="Rob Jones - The Vatican Conspiracy")
    for _ in range(2):
        run(Pipeline().auto_process(dl_id))
        dl = reload(db, dl_id)
        assert not dl.auto_processed and dl.auto_next_try
        dl.auto_next_try = None
        db.commit()
    run(Pipeline().auto_process(dl_id))
    assert reload(db, dl_id).auto_processed is True


def test_restart_while_filing_finishes_instead_of_reconverting(db, settings, tmp_path):
    book = tmp_path / "lib" / "Book.m4b"
    book.parent.mkdir()
    book.write_bytes(b"m4b")
    dl_id = make_download(db, status="downloaded", m4b_status="converting", m4b_path=str(book),
                          download_metadata=json.dumps({"filed": True}))
    stale = make_download(db, status="downloaded", m4b_status="converting")
    Pipeline().recover()
    assert reload(db, dl_id).m4b_status == "converted"
    assert reload(db, stale).m4b_status == "queued"


def test_conversion_marks_filed_and_clears_it_on_the_next_run(db, settings, tmp_path, monkeypatch):
    dl_id, _, _ = _converted_setup(db, settings, tmp_path)
    monkeypatch.setattr(Pipeline, "_run", fake_run_success([]))
    run(Pipeline().convert(dl_id))
    dl = reload(db, dl_id)
    assert dl.m4b_status == "converted" and dl.parsed_metadata.get("filed") is True


def test_stop_during_process_start_kills_it(monkeypatch):
    p = Pipeline()
    killed = []
    monkeypatch.setattr(p, "_kill_current", lambda: killed.append(True))

    class Proc:
        returncode = None
        pid = 1

        class stdout:
            @staticmethod
            async def read(n):
                return b""

        async def wait(self):
            return 0

    async def fake_exec(*cmd, **kw):
        p.cancelled.add(5)  # Stop arrives while the process is starting
        return Proc()
    monkeypatch.setattr(pl.asyncio, "create_subprocess_exec", fake_exec)
    asyncio.run(p._run(5, ["m4b-tool"]))
    assert killed


def test_deleting_a_download_removes_its_log(client, db):
    dl = Download(status="failed")
    db.add(dl)
    db.commit()
    db.add(LogEntry(level="INFO", source="x", message="old book", download_id=dl.id))
    db.commit()
    client.delete(f"/downloads/{dl.id}")
    assert db.query(LogEntry).filter(LogEntry.download_id == dl.id).count() == 0


def test_retry_resets_the_automatic_match_state(client, db, settings, monkeypatch):
    from app.routers import downloads as dl_router
    settings(nzbget_url="http://nzbget")
    dl = Download(status="failed", nzb_hash="h", nzb_name="x",
                  auto_processed=True, auto_attempts=5, review_reason="old",
                  download_metadata=json.dumps({"match_method": "rules", "nzb_retries": 2}))
    db.add(dl)
    db.commit()
    monkeypatch.setattr(dl_router, "fetch_nzb", lambda *a, **k: b"<nzb><file/></nzb>")
    sends = []
    monkeypatch.setattr(dl_router, "submit_to_nzbget", lambda *a, **k: sends.append(k) or 9)
    client.post(f"/downloads/{dl.id}/retry-download")
    db.expire_all()
    row = db.get(Download, dl.id)
    assert (row.auto_processed, row.auto_attempts, row.review_reason) == (False, 0, None)
    assert not ({"match_method", "nzb_retries"} & set(row.parsed_metadata))
    assert sends == [{"force": True}]


def test_notifications_never_raise(monkeypatch):
    def broken():
        raise RuntimeError("database is locked")
    monkeypatch.setattr(notify, "SessionLocal", broken)
    assert notify.send_event_sync("book_converted", title="x") is False
    assert notify.send_for_download("book_converted", 1) is False


def test_odd_plan_readings_are_ignored(db):
    ai._record_windows(db, {"five_hour": {"utilization": "lots", "resetsAt": "soon"}})
    assert ai.plan_status(db) == {}


# ── requests and the forum cache ──────────────────────────────────────────────

def test_split_query_uses_the_last_by():
    assert br.split_query("Murder by Death by Jane Smith") == ("Murder by Death", "Jane Smith")


def test_topic_picker_switch_works_without_the_match_reviewer(db, settings, monkeypatch):
    settings(ai_enabled="0", ai_pick_requests="1", claude_oauth_token="t")
    monkeypatch.setattr(ai, "cli_path", lambda: "claude")
    assert ai.unavailable_reason(db, "ai_pick_requests") == ""
    assert "turned off" in ai.unavailable_reason(db)


def test_empty_searches_are_not_reused(db):
    class Forum:
        import threading
        lock = threading.Lock()
        calls = 0

        def search(self, q):
            Forum.calls += 1
            return []
    forum_cache.search(db, Forum(), "Not Uploaded Yet")
    forum_cache.search(db, Forum(), "Not Uploaded Yet")
    assert Forum.calls == 2


def test_failed_downloads_do_not_block_a_request(db):
    db.add(Download(status="failed", search_term="abook.ws-1"))
    db.commit()
    assert pl.find_duplicates(db, "", search_term="abook.ws-1", include_failed=False) == []
    assert pl.find_duplicates(db, "", search_term="abook.ws-1")  # the Search page still warns


def test_saved_send_again_forces_past_nzbget_duplicate_check(client, db, settings, monkeypatch):
    settings(nzbget_url="http://nzbget")
    post = forum_cache.save_revealed(db, "1", "11", "Topic", {"search_term": "abook.ws-1", "password": "pw"})
    monkeypatch.setattr(nzb_mod, "search_nzbs", lambda source, term: [{"nzb_hash": "h", "title": term, "source": source}])
    monkeypatch.setattr(br, "fetch_nzb", lambda *a: b"<nzb><file/></nzb>")
    forced = []
    monkeypatch.setattr(br, "submit_to_nzbget", lambda db, c, n, p, force=False: forced.append(force) or 3)
    client.post(f"/saved/{post.id}/send")
    assert forced == [True]


def test_deleting_a_saved_post_updates_the_count(client, db):
    post = forum_cache.save_revealed(db, "1", "11", "Topic", {"search_term": "abook.ws-1"})
    r = client.delete(f"/saved/{post.id}")
    assert 'id="saved-total"' in r.text and ">0<" in r.text
    assert db.query(RevealedPost).count() == 0


def test_request_notice_is_out_of_band_and_polling_leaves_it(client):
    r = client.post("/downloads/requests", data={"query": "x"})
    assert 'id="request-notice" hx-swap-oob="true"' in r.text and "Type the book" in r.text
    assert "request-notice" not in client.get("/downloads/requests").text
