"""'Fix & retry' on a failed download: every step editable, the same row follows the retry."""
import json
import threading

from app.models import Download, RevealedPost
from app.routers import downloads as dl_router
from app.routers import search as search_router
from app.services import book_requests, releases
from app.services.nzbget import NzbgetClient


def _failed(db, **fields):
    values = dict(status="failed", nzb_hash="old", nzb_source="nzbking", nzb_title="Old NZB", nzb_name="Book",
                  nzbget_id=12, search_term="abook.link - aaa", password="wrong", topic_id="500", msg_id="1",
                  post_title="Rina Kent - Legacy of Gods 01 - God of Malice (2022)", auto_processed=True,
                  error_message="Archive password was wrong or missing — check the password from the forum post",
                  download_metadata=json.dumps({"title": "God of Malice", "author": "Rina Kent", "nzb_retries": 2}))
    values.update(fields)
    dl = Download(**values)
    db.add(dl)
    db.commit()
    return dl


def _fresh(db, dl_id):
    db.expire_all()
    return db.get(Download, dl_id)


def test_panel_shows_each_step_with_the_forum_posts(client, db, settings):
    settings(nzbget_url="http://nzbget", abook_url="https://abook.example/index.php")
    dl = _failed(db)
    db.add_all([RevealedPost(topic_id="500", msg_id="1", search_term="abook.link - aaa", password="wrong"),
                RevealedPost(topic_id="500", msg_id="2", search_term="abook.link - bbb", password="right",
                             raw_text="M4B Search: abook.link - bbb\nPassword: right")])
    db.commit()
    page = client.get(f"/downloads/{dl.id}/fix").text
    assert "What went wrong" in page and "Usually the password" in page
    assert "In use" in page and 'data-term="abook.link - bbb"' in page and 'data-pw="right"' in page
    assert "Unpack again" in page and "Download it again" in page and "Find other NZBs" in page
    assert "https://abook.example/index.php?topic=500" in page


def test_panel_only_for_failed_downloads(client, db):
    dl = _failed(db, status="sent")
    assert "failed any more" in client.get(f"/downloads/{dl.id}/fix").text


def test_unpack_again_reuses_the_nzbget_job_with_the_new_password(client, db, settings, monkeypatch):
    settings(nzbget_url="http://nzbget")
    dl = _failed(db)
    calls = []

    class FakeNzbget:
        def unpack_again(self, nzb_id, password):
            calls.append((nzb_id, password))
    monkeypatch.setattr(dl_router, "nzbget_client", lambda db: FakeNzbget())
    r = client.post(f"/downloads/{dl.id}/fix/unpack", data={"search_term": "abook.link - aaa", "password": " right "})
    assert r.headers.get("HX-Trigger") == "refreshDownloads"
    assert calls == [(12, "right")]
    row = _fresh(db, dl.id)
    assert (row.status, row.nzbget_id, row.password, row.error_message, row.auto_processed) == \
        ("sent", 12, "right", None, False)


def test_unpack_again_refused_keeps_the_new_password_and_explains(client, db, settings, monkeypatch):
    settings(nzbget_url="http://nzbget")
    dl = _failed(db)

    class Refusing:
        def unpack_again(self, nzb_id, password):
            raise ValueError("NZBGet no longer has job 12 in its history")
    monkeypatch.setattr(dl_router, "nzbget_client", lambda db: Refusing())
    page = client.post(f"/downloads/{dl.id}/fix/unpack", data={"search_term": "abook.link - aaa", "password": "right"}).text
    assert "no longer has job 12" in page and "Download it again instead" in page
    row = _fresh(db, dl.id)
    assert (row.status, row.password) == ("failed", "right")


def test_download_again_sends_the_same_nzb_with_the_edited_password(client, db, settings, monkeypatch):
    settings(nzbget_url="http://nzbget")
    dl = _failed(db)
    monkeypatch.setattr(dl_router, "fetch_nzb", lambda *a, **k: b"<nzb><file/></nzb>")
    sends = []
    monkeypatch.setattr(dl_router, "submit_to_nzbget", lambda db, content, name, password, **k: sends.append(password) or 99)
    r = client.post(f"/downloads/{dl.id}/fix/resend", data={"search_term": "abook.link - aaa", "password": "right"})
    assert "with password right" in r.text and sends == ["right"]
    row = _fresh(db, dl.id)
    assert (row.status, row.nzbget_id, row.password) == ("sent", 99, "right")
    assert "nzb_retries" not in row.parsed_metadata


def test_find_other_nzbs_uses_the_edited_search_word_and_marks_the_failed_one(client, db, settings, monkeypatch):
    settings(nzbget_url="http://nzbget")
    dl = _failed(db)
    searched = []
    monkeypatch.setattr(dl_router, "find_nzbs", lambda term, every_source=False: searched.append(term) or (
        [{"nzb_hash": "old", "title": "Old NZB", "source": "nzbking"},
         {"nzb_hash": "new", "title": "New NZB", "source": "binsearch"}], []))
    page = client.post(f"/downloads/{dl.id}/fix/nzbs", data={"search_term": " abook.link - bbb ", "password": "right"}).text
    assert searched == ["abook.link - bbb"]
    assert "Failed before" in page and "New NZB" in page
    assert 'name="search_term" value="abook.link - bbb"' in page and 'name="password" value="right"' in page
    assert f'name="replace_id" value="{dl.id}"' in page


def test_chosen_replacement_nzb_keeps_its_search_word_and_password(client, db, settings, monkeypatch):
    settings(nzbget_url="http://nzbget")
    dl = _failed(db)
    monkeypatch.setattr(dl_router, "fetch_nzb", lambda *a, **k: b"<nzb><file/></nzb>")
    sends = []
    monkeypatch.setattr(dl_router, "submit_to_nzbget", lambda db, content, name, password, **k: sends.append(password) or 31)
    client.post("/downloads/send", data={"nzb_hash": "new", "nzb_title": "New NZB", "nzb_source": "binsearch",
                                         "search_term": "abook.link - bbb", "password": "", "replace_id": str(dl.id)})
    row = _fresh(db, dl.id)
    assert (row.search_term, row.password, row.nzb_hash, row.nzbget_id) == ("abook.link - bbb", "", "new", 31)
    assert sends == [""]  # emptied on purpose: no password


def test_save_only(client, db):
    dl = _failed(db)
    client.post(f"/downloads/{dl.id}/fix/save", data={"search_term": "abook.link - ccc", "password": "pw2"})
    row = _fresh(db, dl.id)
    assert (row.search_term, row.password, row.status) == ("abook.link - ccc", "pw2", "failed")


def test_read_the_post_again_saves_every_hidden_post(client, db, settings, monkeypatch):
    dl = _failed(db)
    thanked = []

    class FakeScraper:
        lock = threading.Lock()

        def get_topic(self, topic_id):
            return {"topic_id": topic_id, "title": "Legacy of Gods 01", "posts": [
                {"msg_id": "1", "revealed": {"search_term": "abook.link - aaa", "password": "wrong"}},
                {"msg_id": "2", "revealed": None, "has_hidden": True, "thank_href": "?thank=2"},
            ]}

        def thank_and_get_content(self, topic_id, msg_id, thank_href):
            thanked.append(msg_id)
            return {"search_term": "abook.link - bbb", "password": "right"}
    monkeypatch.setattr(book_requests, "_scraper", lambda db: FakeScraper())
    page = client.post(f"/downloads/{dl.id}/fix/reread").text
    assert thanked == ["2"]
    assert "2 post(s) with a search word" in page and 'data-pw="right"' in page
    assert db.query(RevealedPost).filter(RevealedPost.topic_id == "500").count() == 2


def test_prowlarr_result_replaces_a_failed_torrent_in_the_same_row(client, db, settings, monkeypatch):
    settings(nzbget_url="http://nzbget", prowlarr_url="http://prowlarr", prowlarr_api_key="k")
    dl = _failed(db, client="qbittorrent", nzbget_id=None, nzb_source="prowlarr", torrent_hash="abc")
    monkeypatch.setattr(releases, "fetch_nzb", lambda *a, **k: b"<nzb><file/></nzb>")
    monkeypatch.setattr(releases, "submit_to_nzbget", lambda *a, **k: 55)
    release = {"guid": "g1", "title": "God of Malice M4B", "protocol": "usenet", "indexer": "NZBgeek",
               "download_url": "http://prowlarr/dl/1"}
    search_router._remember([release])
    r = client.post("/search/release", data={"guid": "g1", "replace_id": str(dl.id)})
    assert "the download row follows it now" in r.text and r.headers.get("HX-Trigger") == "refreshDownloads"
    row = _fresh(db, dl.id)
    assert (row.status, row.client, row.nzbget_id, row.nzb_source, row.indexer, row.torrent_hash) == \
        ("sent", None, 55, "prowlarr", "NZBgeek", None)
    assert row.parsed_metadata["download_url"] == "http://prowlarr/dl/1"
    assert db.query(Download).count() == 1


def test_failed_row_offers_fix_and_retry(client, db):
    _failed(db)
    rows = client.post("/downloads/sync-status", data={"dl_filter": "active"}).text
    assert "Fix &amp; retry" in rows or "Fix & retry" in rows


def test_nzbget_unpack_again_sets_the_password_then_reprocesses(monkeypatch):
    client = NzbgetClient("http://nzbget", "u", "p")
    calls = []
    monkeypatch.setattr(client, "_rpc", lambda method, params: calls.append((method, params)) or True)
    client.unpack_again(7, "p=w")
    assert calls == [("editqueue", ["HistorySetParameter", "*Unpack:Password=p=w", [7]]),
                     ("editqueue", ["HistoryProcess", "", [7]])]
