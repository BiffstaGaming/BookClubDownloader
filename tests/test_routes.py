import json

from app.models import Download
from app.routers import downloads as dl_router
from app.services import pipeline as pl
from app.settings_store import get_setting


def test_pages_render(client):
    for path in ["/", "/downloads", "/settings", "/logs", "/api/status", "/api/health"]:
        assert client.get(path).status_code == 200, path


def test_settings_never_render_secrets_and_blank_keeps_them(client, db, settings):
    settings(nzbget_password="hunter2", abs_token="abs-secret")
    page = client.get("/settings").text
    assert "hunter2" not in page and "abs-secret" not in page
    assert "saved — leave blank to keep" in page

    form = {"nzbget_url": "http://nzbget:6789", "nzbget_password": "", "abs_token": "new-token",
            "auto_match_threshold": "85"}
    assert client.post("/settings", data=form).status_code == 200
    db.expire_all()
    assert get_setting(db, "nzbget_password") == "hunter2"
    assert get_setting(db, "abs_token") == "new-token"
    assert get_setting(db, "auto_match_threshold") == "85"
    assert get_setting(db, "auto_convert") == "0"  # unticked checkbox


def test_error_messages_are_escaped(client, monkeypatch, settings):
    settings(nzbget_url="http://nzbget")

    def boom(*a, **k):
        raise RuntimeError("<script>alert(1)</script>")
    monkeypatch.setattr(dl_router, "fetch_nzb", boom)
    r = client.post("/downloads/send", data={"nzb_hash": "h", "nzb_title": "t", "force": "1"})
    assert "<script>" not in r.text and "&lt;script&gt;" in r.text


def test_send_warns_about_duplicates_then_force_sends(client, db, settings, monkeypatch):
    settings(nzbget_url="http://nzbget")
    db.add(Download(status="downloaded", nzb_hash="abc", search_term="abook.ws-123", post_title="Old"))
    db.commit()
    monkeypatch.setattr(dl_router, "fetch_nzb", lambda *a: b"<nzb><file/></nzb>")
    monkeypatch.setattr(dl_router, "submit_to_nzbget", lambda *a: 42)

    r = client.post("/downloads/send", data={"nzb_hash": "abc", "nzb_title": "t", "search_term": "abook.ws-123"})
    assert "duplicate" in r.text and 'name="force" value="1"' in r.text
    assert db.query(Download).count() == 1

    r = client.post("/downloads/send", data={"nzb_hash": "abc", "nzb_title": "t", "search_term": "abook.ws-123",
                                             "book_narrator": "Colin Mace", "force": "1"})
    assert "Job ID 42" in r.text
    new = db.query(Download).filter(Download.nzbget_id == 42).one()
    assert new.nzb_source == "nzbking" and new.parsed_metadata["narrator"] == "Colin Mace"


def test_replace_nzb_updates_existing_row(client, db, settings, monkeypatch):
    settings(nzbget_url="http://nzbget")
    dl = Download(status="failed", nzb_hash="old", error_message="damaged", password="pw",
                  download_metadata=json.dumps({"title": "T", "auto_processed": True}))
    db.add(dl)
    db.commit()
    monkeypatch.setattr(dl_router, "fetch_nzb", lambda *a: b"<nzb><file/></nzb>")
    monkeypatch.setattr(dl_router, "submit_to_nzbget", lambda *a: 77)
    r = client.post("/downloads/send", data={"nzb_hash": "new", "nzb_title": "t", "nzb_source": "binsearch",
                                             "replace_id": str(dl.id)})
    assert r.status_code == 200 and r.headers.get("HX-Trigger") == "refreshDownloads"
    db.expire_all()
    dl = db.get(Download, dl.id)
    assert (dl.status, dl.nzbget_id, dl.nzb_hash, dl.nzb_source, dl.error_message) == ("sent", 77, "new", "binsearch", None)
    assert "auto_processed" not in dl.parsed_metadata
    assert db.query(Download).count() == 1


def test_apply_match_and_queue_conversion(client, db, tmp_path):
    src = tmp_path / "book"
    src.mkdir()
    (src / "a.mp3").write_bytes(b"x")
    dl = Download(status="downloaded", download_path=str(src), post_title="Some Post",
                  download_metadata=json.dumps({"series": "Wrong Series"}))
    db.add(dl)
    db.commit()
    candidate = {"title": "Right Book", "author": "Author A", "narrator": "Reader R", "asin": "B01",
                 "series": "", "confidence": 95}
    r = client.post(f"/downloads/{dl.id}/apply-match", data={"candidate": json.dumps(candidate)})
    assert "Right Book" in r.text
    db.expire_all()
    meta = db.get(Download, dl.id).parsed_metadata
    assert meta["asin"] == "B01" and "series" not in meta  # stale field from the previous match removed

    form = {"title": "Right Book", "author": "Author A", "narrator": "Reader R", "input_path": str(src)}
    r = client.post(f"/downloads/{dl.id}/convert", data=form)
    assert "queued" in r.text
    db.expire_all()
    assert db.get(Download, dl.id).m4b_status == "queued"


def test_convert_rejects_missing_folder(client, db):
    dl = Download(status="downloaded")
    db.add(dl)
    db.commit()
    r = client.post(f"/downloads/{dl.id}/convert", data={"title": "T", "author": "A", "input_path": "/no/such/dir"})
    assert "does not exist" in r.text
    db.expire_all()
    assert db.get(Download, dl.id).m4b_status is None


def test_reset_conversion_views(client, db):
    dl = Download(status="downloaded", m4b_status="queued")
    db.add(dl)
    db.commit()
    assert "<tr>" in client.post(f"/downloads/{dl.id}/reset-conversion").text
    assert "<form" in client.post(f"/downloads/{dl.id}/reset-conversion?view=form").text


def test_active_filter_shows_failures_hides_finished(client, db):
    db.add_all([
        Download(post_title="Failed one", status="failed"),
        Download(post_title="Finished one", status="downloaded", m4b_status="converted", abs_status="imported"),
        Download(post_title="Import problem", status="downloaded", m4b_status="converted", abs_status="not_found"),
    ])
    db.commit()
    html = client.post("/downloads/sync-status", data={"dl_filter": "active", "dl_limit": 0}).text
    assert "Failed one" in html and "Import problem" in html and "Finished one" not in html


def test_status_api(client, db):
    db.add_all([Download(status="sent"), Download(status="downloaded", m4b_status="queued", post_title="Q")])
    db.commit()
    data = client.get("/api/status").json()
    assert data["downloading"] == 1 and data["queued"] == 1 and data["queue"] == ["Q"]
    assert data["state"] == "downloading"


def test_metadata_lookup_shows_candidates(client, db, settings, monkeypatch):
    settings(abs_url="http://abs", abs_token="t")
    dl = Download(status="downloaded", post_title="Rob Jones - The Vatican Conspiracy")
    db.add(dl)
    db.commit()

    class Abs:
        def __init__(self, *a):
            pass

        def search_books(self, title, author="", provider="audible"):
            return [{"title": "The Vatican Conspiracy", "author": "Rob Jones", "asin": "B07"}]
    monkeypatch.setattr(dl_router, "AbsClient", Abs)
    html = client.get(f"/downloads/{dl.id}/metadata-lookup").text
    assert "Use this" in html and "B07" in html


def test_home_assistant_test_button(client, clean_db):
    r = client.post("/settings/test-ha", data={"ha_webhook_url": "http://ha/api/webhook/x"})
    assert "Sent" in r.text and clean_db[0]["event"] == "test"


def test_pipeline_find_duplicates_against_abs(db, settings, monkeypatch):
    settings(abs_url="http://abs", abs_token="t", abs_library_id="lib")

    class Abs:
        def __init__(self, *a):
            pass

        def search_library(self, lib, q):
            return [{"id": "1", "media": {"metadata": {"title": "Dune", "authorName": "Frank Herbert"}}}]
    monkeypatch.setattr(pl, "AbsClient", Abs)
    reasons = pl.find_duplicates(db, "Dune", "Frank Herbert")
    assert reasons and "Audiobookshelf" in reasons[0]
    assert pl.find_duplicates(db, "Dune Messiah", "Frank Herbert") == []


def test_health_reports_dead_background_task(client, monkeypatch):
    class Done:
        def done(self):
            return True

        def get_name(self):
            return "bookclub-worker"
    monkeypatch.setattr(pl.pipeline, "tasks", [Done()])
    r = client.get("/api/health")
    assert r.status_code == 503 and r.json()["stopped"] == ["bookclub-worker"]
