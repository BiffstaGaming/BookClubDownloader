"""Choosing NZBs, trying another NZB after a failure, Prowlarr, qBittorrent, and the request fallback."""
import asyncio
import json

import pytest

from app.models import BookRequest, Download
from app.services import book_requests as br
from app.services import nzb as nzb_mod
from app.services import pipeline as pl
from app.services import prowlarr, qbittorrent, releases


def run(coro):
    return asyncio.run(coro)


# ── ranking NZBs ──────────────────────────────────────────────────────────────

def test_size_and_age_parsing():
    assert nzb_mod.parse_size_mb("258MB") == 258
    assert nzb_mod.parse_size_mb("size: 1.5 GB") == 1536
    assert nzb_mod.parse_size_mb("700 KiB") == pytest.approx(700 / 1024)
    assert nzb_mod.parse_size_mb("") is None
    assert nzb_mod.parse_age_days("26d") == 26
    assert nzb_mod.parse_age_days("3 hours") == pytest.approx(0.125)
    assert nzb_mod.parse_age_days("1 year") == 365
    assert nzb_mod.parse_age_days("5 min") == pytest.approx(5 / 1440)


def test_rank_prefers_m4b_then_complete_and_newest():
    results = [
        {"nzb_hash": "small-new", "title": "book", "size": "100MB", "age": "1d"},
        {"nzb_hash": "full-old", "title": "book", "size": "500MB", "age": "900d"},
        {"nzb_hash": "full-new", "title": "book", "size": "495MB", "age": "20d"},
        {"nzb_hash": "m4b", "title": "book m4b", "size": "300MB", "age": "2000d"},
    ]
    assert [r["nzb_hash"] for r in nzb_mod.rank_nzbs(results)] == ["m4b", "full-new", "full-old", "small-new"]


def test_find_nzbs_asks_binsearch_only_when_needed(monkeypatch):
    asked = []
    monkeypatch.setattr(nzb_mod, "search_nzbs", lambda source, term: asked.append(source) or [
        {"nzb_hash": source, "title": term, "source": source, "size": "1MB"}])
    nzb_mod.find_nzbs("t")
    assert asked == ["nzbking"]
    asked.clear()
    found, _ = nzb_mod.find_nzbs("t", every_source=True)
    assert asked == ["nzbking", "binsearch"] and len(found) == 2


# ── another NZB after a failure ───────────────────────────────────────────────

class FakeNzbget:
    history, queue = [], []

    def __init__(self, *a, **k):
        pass

    def get_history(self):
        return FakeNzbget.history

    def get_queue(self):
        return FakeNzbget.queue


@pytest.fixture
def retry_env(monkeypatch, settings):
    settings(nzbget_url="http://nzbget", ha_webhook_url="http://ha/hook")
    FakeNzbget.history, FakeNzbget.queue = [], []
    monkeypatch.setattr(pl, "NzbgetClient", FakeNzbget)
    monkeypatch.setattr(nzb_mod, "search_nzbs", lambda source, term: [
        {"nzb_hash": f"{source}-a", "title": term, "source": source, "size": "500MB", "age": "10d"},
        {"nzb_hash": f"{source}-b", "title": term, "source": source, "size": "100MB", "age": "10d"}])
    monkeypatch.setattr(pl, "fetch_nzb", lambda source, h, title="", url="": b"<nzb><file/></nzb>")
    sent = []
    monkeypatch.setattr(pl, "submit_to_nzbget", lambda db, content, name, password="", force=False:
                        sent.append((name, password, force)) or 100 + len(sent))
    return sent


def test_unpack_failure_sends_the_next_nzb(db, retry_env, clean_db):
    dl = Download(status="sent", nzbget_id=7, nzb_hash="nzbking-a", nzb_source="nzbking", nzb_name="Book",
                  search_term="abook.ws-1", password="pw", auto_processed=True)
    db.add(dl)
    db.commit()
    FakeNzbget.history = [{"NZBID": 7, "Status": "FAILURE/UNPACK"}]
    run(pl.Pipeline().poll_once())
    db.expire_all()
    dl = db.get(Download, dl.id)
    assert (dl.status, dl.nzbget_id, dl.nzb_hash, dl.error_message) == ("sent", 101, "binsearch-a", None)
    assert dl.auto_processed is False  # a fresh download gets a fresh automatic match
    meta = dl.parsed_metadata
    assert meta["nzb_retries"] == 1 and set(meta["tried_nzbs"]) == {"nzbking-a", "binsearch-a"}
    assert retry_env == [("Book", "pw", True)]
    events = [e["event"] for e in clean_db]
    assert "download_retrying" in events and "download_failed" not in events


def test_retries_stop_at_the_limit_and_skip_wrong_passwords(db, retry_env, settings, clean_db):
    settings(nzb_auto_retry="1")
    dl = Download(status="sent", nzbget_id=8, nzb_hash="x", nzb_source="nzbking", search_term="t",
                  download_metadata=json.dumps({"nzb_retries": 1}))
    wrong_password = Download(status="sent", nzbget_id=9, nzb_hash="y", nzb_source="nzbking", search_term="t")
    db.add_all([dl, wrong_password])
    db.commit()
    FakeNzbget.history = [{"NZBID": 8, "Status": "FAILURE/HEALTH"}, {"NZBID": 9, "Status": "WARNING/PASSWORD"}]
    run(pl.Pipeline().poll_once())
    db.expire_all()
    assert db.get(Download, dl.id).status == "failed" and db.get(Download, wrong_password.id).status == "failed"
    assert retry_env == []
    assert [e["event"] for e in clean_db].count("download_failed") == 2


# ── Prowlarr ──────────────────────────────────────────────────────────────────

PROWLARR = [
    {"guid": "g1", "title": "Andy Weir - Project Hail Mary (2021) [M4B]", "size": 600 * 1024 ** 2, "age": 30,
     "indexer": "NZBgeek", "protocol": "usenet", "downloadUrl": "http://prowlarr/dl/1?apikey=k"},
    {"guid": "g2", "title": "Project Hail Mary - Andy Weir", "size": 700 * 1024 ** 2, "ageHours": 48,
     "indexer": "MyTracker", "protocol": "torrent", "downloadUrl": "http://prowlarr/dl/2", "magnetUrl": "",
     "seeders": 12},
    {"guid": "g3", "title": "Something Else Entirely", "size": 1, "protocol": "usenet", "downloadUrl": "x"},
]


def test_prowlarr_results_are_normalised_and_ranked():
    results = [prowlarr.normalize_result(r) for r in PROWLARR]
    assert results[1]["protocol"] == "torrent" and results[1]["age_days"] == 2
    ranked = prowlarr.rank_releases(results, "Project Hail Mary", "Andy Weir", prefer="usenet")
    assert [r["guid"] for r in ranked][:2] == ["g1", "g2"] and ranked[-1]["guid"] == "g3"
    assert ranked[0]["score"] >= prowlarr.CONFIDENT_SCORE > ranked[-1]["score"]
    assert ranked[0]["size_label"] == "600 MB"


def test_prowlarr_client_query(monkeypatch):
    calls = []

    class Resp:
        status_code = 200

        def raise_for_status(self):
            pass

        def json(self):
            return PROWLARR

    monkeypatch.setattr(prowlarr.requests, "get", lambda url, **k: calls.append((url, k)) or Resp())
    results = prowlarr.ProwlarrClient("http://prowlarr:9696/", "key").search("Project Hail Mary")
    url, kwargs = calls[0]
    assert url == "http://prowlarr:9696/api/v1/search" and kwargs["headers"] == {"X-Api-Key": "key"}
    assert kwargs["params"]["categories"] == [prowlarr.AUDIOBOOK_CATEGORY] and kwargs["params"]["type"] == "search"
    assert len(results) == 3


# ── qBittorrent ───────────────────────────────────────────────────────────────

def test_torrent_states():
    assert qbittorrent.torrent_state({"state": "downloading", "progress": 0.42}) == ("downloading", 42)
    assert qbittorrent.torrent_state({"state": "stoppedUP", "progress": 1}) == ("downloaded", 100)
    assert qbittorrent.torrent_state({"state": "stalledUP", "progress": 0.999}) == ("downloaded", 100)
    assert qbittorrent.torrent_state({"state": "missingFiles", "progress": 0.5})[0] == "failed"


class FakeQbit:
    added, torrents_list = [], []

    def __init__(self, *a, **k):
        pass

    def add(self, tag, urls="", torrent=None, category=""):
        FakeQbit.added.append((tag, urls, torrent, category))

    def torrents(self, tag="", hashes=None):
        return FakeQbit.torrents_list


def test_torrent_is_sent_followed_and_never_deleted(db, settings, monkeypatch, tmp_path):
    settings(qbit_url="http://qbit:8080", qbit_category="books", qbit_path_prefix="/data", qbit_local_prefix=str(tmp_path))
    FakeQbit.added, FakeQbit.torrents_list = [], []
    monkeypatch.setattr(releases, "QbitClient", FakeQbit)
    monkeypatch.setattr(releases, "fetch_torrent", lambda url: ("magnet:?xt=urn:btih:abc", None))
    release = prowlarr.normalize_result(PROWLARR[1])
    dl = releases.send_release(db, release, {"title": "Project Hail Mary", "author": "Andy Weir"})
    assert dl.is_torrent and dl.status == "sent" and dl.indexer == "MyTracker"
    assert FakeQbit.added == [(f"bookclub-{dl.id}", "magnet:?xt=urn:btih:abc", None, "books")]

    folder = tmp_path / "Project Hail Mary"
    folder.mkdir()
    monkeypatch.setattr(pl.releases, "qbit_client", lambda db: FakeQbit())
    FakeQbit.torrents_list = [{"hash": "abc", "tags": f"bookclub-{dl.id}", "state": "downloading", "progress": 0.5}]
    run(pl.Pipeline().poll_once())
    db.expire_all()
    assert db.get(Download, dl.id).download_progress == 50
    FakeQbit.torrents_list = [{"hash": "abc", "tags": f"other, bookclub-{dl.id}", "state": "uploading", "progress": 1,
                               "content_path": "/data/Project Hail Mary", "save_path": "/data"}]
    run(pl.Pipeline().poll_once())
    db.expire_all()
    row = db.get(Download, dl.id)
    assert row.status == "downloaded" and row.torrent_hash == "abc"
    assert row.download_path.replace("\\", "/").endswith("/Project Hail Mary")


def test_usenet_result_goes_to_nzbget_with_its_link(db, settings, monkeypatch):
    settings(nzbget_url="http://nzbget")
    fetched = []
    monkeypatch.setattr(releases, "fetch_nzb", lambda source, h, t="", url="": fetched.append((source, url)) or b"nzb")
    monkeypatch.setattr(releases, "submit_to_nzbget", lambda db, content, name, password="", force=False: 55)
    dl = releases.send_release(db, prowlarr.normalize_result(PROWLARR[0]), {"title": "Project Hail Mary"})
    assert fetched == [("prowlarr", "http://prowlarr/dl/1?apikey=k")]
    assert (dl.nzbget_id, dl.nzb_source, dl.client) == (55, "prowlarr", None)
    assert dl.parsed_metadata["download_url"] == "http://prowlarr/dl/1?apikey=k"


def test_torrent_needs_qbittorrent(db):
    with pytest.raises(Exception, match="qBittorrent is not set up"):
        releases.send_release(db, prowlarr.normalize_result(PROWLARR[1]))


# ── requests fall back to Prowlarr ────────────────────────────────────────────

@pytest.fixture
def forum_without_the_book(monkeypatch, settings):
    settings(abook_url="http://f", abook_username="u", abook_password="p", nzbget_url="http://nzbget",
             prowlarr_url="http://prowlarr", prowlarr_api_key="k")
    monkeypatch.setattr(br, "_scraper", lambda db: object())
    monkeypatch.setattr(br, "_search_forum", lambda db, scraper, query, refresh=False: [])


def test_request_not_on_the_forum_is_found_on_indexers(db, forum_without_the_book, monkeypatch):
    monkeypatch.setattr(releases, "search", lambda db, q: prowlarr.rank_releases(
        [prowlarr.normalize_result(r) for r in PROWLARR], "Project Hail Mary", "Andy Weir"))
    sent = []

    def fake_send(db, release, book, requested_by=None, request_id=None):
        dl = Download(status="sent", post_title=release["title"], request_id=request_id)
        db.add(dl)
        db.commit()
        sent.append(release["guid"])
        return dl
    monkeypatch.setattr(releases, "send_release", fake_send)
    req = br.create_request(db, "Project Hail Mary by Andy Weir")
    event, fields = br.run_request(req.id)
    db.expire_all()
    req = db.get(BookRequest, req.id)
    assert event == "request_sent" and req.status == "sent" and sent == ["g1"]
    assert "Not on the forum" in req.message and req.download_id == fields["download_id"]


def test_unsure_indexer_results_are_left_to_you(db, forum_without_the_book, monkeypatch):
    monkeypatch.setattr(releases, "search", lambda db, q: prowlarr.rank_releases(
        [prowlarr.normalize_result(PROWLARR[2])], "Project Hail Mary", "Andy Weir"))
    req = br.create_request(db, "Project Hail Mary by Andy Weir")
    event, _ = br.run_request(req.id)
    db.expire_all()
    req = db.get(BookRequest, req.id)
    assert event == "request_not_found" and req.status == "not_found"
    assert "Prowlarr found 1 result" in req.message and "Search" in req.message


def test_indexer_search_page_never_shows_the_api_key(client, settings, monkeypatch):
    from app.routers import search as search_router
    settings(prowlarr_url="http://prowlarr", prowlarr_api_key="secret-key", nzbget_url="http://nzbget")
    monkeypatch.setattr(search_router.releases, "search", lambda db, q: prowlarr.rank_releases(
        [prowlarr.normalize_result(dict(PROWLARR[0], downloadUrl="http://prowlarr/dl/1?apikey=secret-key"))],
        "Project Hail Mary", "Andy Weir"))
    page = client.post("/search/indexers", data={"query": "Project Hail Mary by Andy Weir"}).text
    assert "Andy Weir - Project Hail Mary" in page and "secret-key" not in page and 'name="guid" value="g1"' in page
    assert "Good match" in page and "Usenet" in page

    sent = []
    monkeypatch.setattr(search_router.releases, "send_release", lambda db, r, book: sent.append((r["guid"], book))
                        or Download(status="sent"))
    r = client.post("/search/release", data={"guid": "g1", "book_title": "Project Hail Mary", "book_author": "Andy Weir"})
    assert "Sent to NZBGet" in r.text and sent == [("g1", {"title": "Project Hail Mary", "author": "Andy Weir"})]
    assert "expired" in client.post("/search/release", data={"guid": "nope"}).text


def test_search_page_offers_indexers_only_when_set_up(client, settings):
    assert "Search your indexers" not in client.get("/").text
    settings(prowlarr_url="http://prowlarr", prowlarr_api_key="k")
    assert "Search your indexers" in client.get("/").text
    assert "Search your indexers" in client.get("/m/search").text
