"""Wishlist (books, authors, followed series), series gaps, the library health check, and their API."""
import json
from datetime import datetime, timedelta

import pytest

from app.models import BookRequest, Download, WishlistItem
from app.services import health, wishlist
from app.services.abs import item_series

TOPICS = {
    "royal elite": [
        {"topic_id": "11", "title": "Book Club - Rina Kent - Royal Elite 01 - Cruel King (Narr/2020)"},
        {"topic_id": "12", "title": "Book Club - Rina Kent - Royal Elite 02 - Deviant King (Narr/2020)"},
        {"topic_id": "13", "title": "Book Club - Rina Kent - Royal Elite 03 - Steel Princess (Narr/2021)"},
        {"topic_id": "99", "title": "Book Club - Someone Else - Other Series 03 - Not This"},
    ],
    "rina kent": [
        {"topic_id": "11", "title": "Book Club - Rina Kent - Royal Elite 01 - Cruel King (Narr/2020)"},
        {"topic_id": "20", "title": "Book Club - Rina Kent - God of Malice (Narr/2022)"},
    ],
    "project hail mary": [{"topic_id": "30", "title": "Book Club - Andy Weir - Project Hail Mary (Ray Porter/2021)"}],
}


@pytest.fixture
def forum(monkeypatch, settings):
    settings(abook_url="http://f", abook_username="u", abook_password="p")
    asked = []

    def search(db, scraper, query, refresh=False):
        asked.append((query, refresh))
        return list(TOPICS.get(query.lower(), [])), None
    monkeypatch.setattr(wishlist.forum_cache, "search", search)
    monkeypatch.setattr(wishlist.book_requests, "_scraper", lambda db: object())
    return asked


def test_series_numbers_from_topic_titles():
    assert wishlist.parse_series_topic("Book Club - Rob Jones - Hunter Files 06 - The Title (Colin Mace/2021)") == \
        ("Hunter Files", "6")
    assert wishlist.parse_series_topic("Book Club - A Author - Expanse Book 03 - Title") == ("Expanse", "3")
    assert wishlist.parse_series_topic("Book Club - A Author - Saga #4.5 - Title") == ("Saga", "4.5")
    assert wishlist.parse_series_topic("Book Club - Andy Weir - Project Hail Mary (2021)") == ("", "")
    assert wishlist.same_series("The Royal Elite", "Royal Elite") and not wishlist.same_series("Royal Elite", "Elite Force")


def test_abs_item_series_shapes():
    assert item_series({"media": {"metadata": {"series": [{"name": "A", "sequence": "2"}]}}}) == [("A", "2")]
    assert item_series({"media": {"metadata": {"seriesName": "The Expanse #3, Other"}}}) == [("The Expanse", "3"), ("Other", "")]


def test_followed_series_requests_the_books_you_dont_have(db, forum, settings):
    settings(wishlist_max_per_check="1")
    db.add(Download(status="downloaded", download_metadata=json.dumps({"series": "Royal Elite", "series_part": "1"})))
    db.commit()
    item, error = wishlist.add_item(db, "series", "Royal Elite", auto_download=True)
    assert not error
    request_ids, notices = wishlist.check_now(item.id)
    db.expire_all()
    req = db.get(BookRequest, request_ids[0])
    assert len(request_ids) == 1 and req.topic_id == "12" and req.query == "Deviant King by Rina Kent"
    item = db.get(WishlistItem, item.id)
    assert "12" in item.seen and "1 more next time" in item.message and item.next_check_at > datetime.utcnow()
    assert notices[0][0] == "wishlist_found"
    assert ("Royal Elite", True) in forum  # the forum is asked afresh, not from the cache

    # Next check: book 2 is requested already, book 3 comes next
    request_ids, _ = wishlist.check_now(item.id)
    db.expire_all()
    assert db.get(BookRequest, request_ids[0]).topic_id == "13"


def test_author_watch_only_reports_new_uploads(db, forum):
    item, _ = wishlist.add_item(db, "author", "Rina Kent", auto_download=False)
    assert wishlist.check_now(item.id) == ([], [])  # first look: what's there isn't news
    db.expire_all()
    assert "2 already on the forum" in db.get(WishlistItem, item.id).message
    TOPICS["rina kent"].append({"topic_id": "21", "title": "Book Club - Rina Kent - New One (Narr/2026)"})
    try:
        request_ids, notices = wishlist.check_now(item.id)
    finally:
        TOPICS["rina kent"].pop()
    db.expire_all()
    item = db.get(WishlistItem, item.id)
    assert request_ids == [] and [t["topic_id"] for t in item.found_topics] == ["21"]
    assert notices and "1 new" in notices[0][1]["message"]

    req = wishlist.take_found(db, item, "21")
    assert req.topic_id == "21" and item.found_topics == [] and "21" in item.seen


def test_wished_book_is_requested_once_it_appears(db, forum):
    item, _ = wishlist.add_item(db, "book", "Project Hail Mary by Andy Weir")
    ids, _ = wishlist.check_now(item.id)
    db.expire_all()
    item = db.get(WishlistItem, item.id)
    assert item.status == "requested" and item.request_id == ids[0]
    req = db.get(BookRequest, ids[0])
    req.status, req.message = "sent", "Downloading"
    db.commit()
    wishlist.check_due()
    db.expire_all()
    assert db.get(WishlistItem, item.id).status == "done"


def test_wishlist_due_checks_and_duplicates(db, forum):
    a, _ = wishlist.add_item(db, "book", "Not On The Forum Yet")
    assert wishlist.add_item(db, "book", "not on the forum yet!")[1].startswith("Already on your wishlist")
    paused, _ = wishlist.add_item(db, "author", "Paused Author")
    paused.status = "paused"
    later, _ = wishlist.add_item(db, "author", "Later Author")
    later.next_check_at = datetime.utcnow() + timedelta(hours=3)
    db.commit()
    forum.clear()
    wishlist.check_due()
    asked = {q for q, _ in forum}
    assert "Not On The Forum Yet" in asked and "Paused Author" not in asked and "Later Author" not in asked
    db.expire_all()
    assert db.get(WishlistItem, a.id).message == "Not on the forum yet"


def test_wishlist_page_and_actions(client, db, forum, monkeypatch):
    from app.services.pipeline import pipeline
    scheduled = []
    monkeypatch.setattr(pipeline, "run_soon", lambda fn, *args: scheduled.append((fn.__name__, args)))
    page = client.get("/wishlist").text
    assert "Watch the forum for" in page and 'hx-get="/wishlist/rows"' in page
    r = client.post("/wishlist", data={"kind": "author", "query": "Rina Kent", "auto_download": "1"})
    assert "Rina Kent" in r.text and "Watching the forum" in r.text and scheduled[0][0] == "check_wishlist_item"
    item = db.query(WishlistItem).one()
    assert "Paused" in client.post(f"/wishlist/{item.id}/pause").text
    assert "Just tell me" not in client.post(f"/wishlist/{item.id}/auto").text  # now "Download automatically"
    assert "Rina Kent" not in client.delete(f"/wishlist/{item.id}").text
    assert "Wishlist" in client.get("/m/wishlist").text and 'href="/m/wishlist"' in client.get("/m").text


# ── series in the library ─────────────────────────────────────────────────────

class FakeAbs:
    def __init__(self, *a, **k):
        pass

    def all_series(self, library_id):
        return [
            {"name": "Royal Elite", "books": [
                {"sequence": "1", "media": {"metadata": {"authorName": "Rina Kent"}}},
                {"sequence": "2", "media": {"metadata": {"authorName": "Rina Kent"}}},
                {"sequence": "5", "media": {"metadata": {"authorName": "Rina Kent"}}}]},
            {"name": "Complete Saga", "books": [{"sequence": "1", "media": {"metadata": {}}}]},
        ]

    def search_series(self, library_id, query):
        return [{"series": {"name": "Royal Elite"}, "books": [{"sequence": "1"}, {"sequence": "2"}]}]

    def library_items(self, library_id):
        return LIBRARY

    def update_media(self, item_id, payload):
        UPDATES.append((item_id, payload))

    def quick_match(self, item_id, title="", author="", provider="audible", asin=""):
        return {"updated": True}


UPDATES = []
LIBRARY = [
    {"id": "a", "size": 300_000_000, "media": {"coverPath": "/c.jpg", "duration": 36000, "numAudioFiles": 1,
                                                "metadata": {"title": "Book A", "authorName": "X", "asin": "B1",
                                                             "seriesName": "Saga #1"}}},
    {"id": "b", "size": 300_000_000, "media": {"coverPath": None, "duration": 36000, "numAudioFiles": 1,
                                                "metadata": {"title": "Book A", "authorName": "X", "asin": "",
                                                             "seriesName": "Saga #1"}}},
    {"id": "c", "size": 20_000_000, "media": {"coverPath": "/c.jpg", "duration": 36000, "numAudioFiles": 12,
                                               "metadata": {"title": "Book C", "authorName": "", "asin": "B3",
                                                            "seriesName": "Other Saga, Saga"}}},
]


@pytest.fixture
def abs_library(monkeypatch, settings):
    settings(abs_url="http://abs", abs_token="t", abs_library_id="lib")
    UPDATES.clear()
    monkeypatch.setattr(health, "AbsClient", FakeAbs)
    monkeypatch.setattr(wishlist, "AbsClient", FakeAbs)


def test_series_gaps(db, abs_library):
    series = health.library_series(db)
    assert series[0]["name"] == "Royal Elite" and series[0]["gaps"] == ["3", "4"] and series[0]["have"] == ["1", "2", "5"]
    assert series[1]["gaps"] == []
    assert wishlist.owned_numbers(db, "Royal Elite") == {"1", "2"}


def test_series_tab_and_forum_check(client, db, abs_library, forum):
    page = client.get("/wishlist/series").text
    assert "Royal Elite" in page and 'class="bc-number missing" title="Missing">#3' in page and "Follow" in page
    forum_page = client.get("/wishlist/series/forum", params={"name": "Royal Elite"}).text
    assert "Have it" in forum_page and "Steel Princess" in forum_page and "/wishlist/request-topic" in forum_page
    assert "Not This" not in forum_page


def test_health_check_finds_problems(db, abs_library):
    health.run_scan()
    data = health.load(db)
    kinds = {(i["item_id"], i["kind"]) for i in data["issues"]}
    assert ("b", "no_cover") in kinds and ("b", "no_asin") in kinds
    assert ("a", "duplicate") in kinds and ("b", "duplicate") in kinds
    assert ("a", "series_duplicate_number") in kinds
    assert ("c", "low_bitrate") in kinds and ("c", "many_files") in kinds and ("c", "no_author") in kinds
    assert ("c", "series_no_number") in kinds
    assert data["total"] == 3 and not data["running"]


def test_claude_suggestions_are_checked_and_applied_only_when_approved(db, abs_library, monkeypatch):
    health.run_scan()
    from app.services import ai

    class Reply:
        text = json.dumps({"suggestions": [
            {"item_id": "c", "field": "sequence", "value": "2", "series": "Saga", "reason": "Second book"},
            {"item_id": "c", "field": "author", "value": "Jane Doe", "reason": "Known author"},
            {"item_id": "zzz", "field": "title", "value": "Injected", "reason": "not a book we sent"},
            {"item_id": "c", "field": "description", "value": "x", "reason": "field not allowed"},
            {"item_id": "c", "field": "sequence", "value": "two", "reason": "not a number"},
        ]})
    monkeypatch.setattr(ai, "ask", lambda db, prompt, switch="ai_enabled": Reply())
    assert "2 fix" in health.suggest(db)
    assert UPDATES == []  # nothing changes before you approve
    message = health.apply_suggestion(db, "c:sequence")
    assert "sequence" in message
    # The other series is kept; only the un-numbered one gets the number
    assert UPDATES[-1] == ("c", {"metadata": {"series": [{"name": "Other Saga", "sequence": ""},
                                                         {"name": "Saga", "sequence": "2"}]}})
    health.apply_suggestion(db, "c:author")
    assert UPDATES[-1] == ("c", {"metadata": {"authors": [{"name": "Jane Doe"}]}})
    assert health.load(db)["suggestions"] == []


def test_library_page(client, db, abs_library):
    assert "Check the library" in client.get("/library").text
    assert "Not checked yet" in client.get("/library/health").text
    health.run_scan()
    page = client.get("/library/health").text
    assert "Quick Match" in page and "No cover (1)" in page and "Ask Claude to suggest fixes" in page
    assert "Quick Match done" in client.post("/library/health/match", data={"item_id": "b"}).text


# ── API for Home Assistant ────────────────────────────────────────────────────

def test_wishlist_and_webhook_api(client, db, monkeypatch, clean_db, settings):
    from app.routers.api import ensure_api_token
    from app.services import notify
    from app.services.pipeline import pipeline
    monkeypatch.setattr(pipeline, "run_soon", lambda *a: None)
    headers = {"Authorization": f"Bearer {ensure_api_token(db)}"}
    assert client.get("/api/wishlist").status_code == 401
    r = client.post("/api/wishlist", json={"query": "Rina Kent", "kind": "author", "auto_download": False}, headers=headers)
    assert r.status_code == 201 and r.json()["kind"] == "author" and r.json()["auto_download"] is False
    assert client.post("/api/wishlist", json={"query": "Rina Kent", "kind": "author"}, headers=headers).status_code == 409
    assert client.post("/api/wishlist", json={"query": "X Y", "kind": "nope"}, headers=headers).status_code == 422
    assert [w["query"] for w in client.get("/api/wishlist", headers=headers).json()] == ["Rina Kent"]
    status = client.get("/api/status").json()
    assert status["wishlist_watching"] == 1 and "encoding_waiting" in status

    assert client.put("/api/integration/webhook", json={"url": "http://ha:8123/api/webhook/abc"},
                      headers=headers).status_code == 200
    settings(ha_webhook_url="http://ha/own", ha_events="book_imported")
    notify.send_event_sync("download_failed", title="T")      # only the integration wants this one
    notify.send_event_sync("book_imported", title="T")        # both get this
    events = [e["event"] for e in clean_db]
    assert events == ["download_failed", "book_imported", "book_imported"]
    assert client.delete("/api/integration/webhook", headers=headers).json() == {"url": ""}
    assert client.delete(f"/api/wishlist/{r.json()['id']}", headers=headers).status_code == 200
