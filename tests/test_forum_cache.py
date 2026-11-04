"""Forum search cache, the local topic index, saved revealed posts and the Saved page."""
import json
import threading
from datetime import datetime, timedelta

import pytest

from app.models import BookRequest, Download, ForumSearch, RevealedPost
from app.routers import search as search_router
from app.services import book_requests as br, forum_cache
from app.settings_store import set_setting
from app.services import nzb as nzb_mod


class CountingForum:
    def __init__(self, topics=None, posts=None):
        self.lock = threading.Lock()
        self.topics = topics if topics is not None else [{"topic_id": "1", "title": "Book Club - Andy Weir - Project Hail Mary"}]
        self.posts = posts or {}
        self.calls = []

    def search(self, query):
        self.calls.append(("search", query))
        return self.topics

    def get_topic(self, topic_id):
        self.calls.append(("topic", topic_id))
        return {"topic_id": topic_id, "title": self.topics[0]["title"], "posts": self.posts.get(topic_id, [])}

    def thank_and_get_content(self, topic_id, msg_id, href):
        self.calls.append(("thank", msg_id))
        return {"search_term": "abook.ws-1", "password": "pw", "title": "Project Hail Mary", "author": "Andy Weir",
                "raw_text": "Title: Project Hail Mary"}


REVEALED = {"search_term": "abook.ws-1", "password": "pw", "title": "Project Hail Mary", "author": "Andy Weir",
            "narrator": "Ray Porter", "series": "", "series_part": ""}


def test_repeat_search_uses_the_cache_until_refresh(db):
    forum = CountingForum()
    results, cached_at = forum_cache.search(db, forum, "Hail Mary")
    assert cached_at is None and results[0]["topic_id"] == "1"
    results, cached_at = forum_cache.search(db, forum, "  hail   MARY ")
    assert cached_at is not None and results[0]["title"].endswith("Project Hail Mary")
    assert len(forum.calls) == 1
    forum_cache.search(db, forum, "Hail Mary", refresh=True)
    assert len(forum.calls) == 2


def test_cache_expires_and_can_be_turned_off(db):
    forum = CountingForum()
    forum_cache.search(db, forum, "Hail Mary")
    row = db.get(ForumSearch, "hail mary")
    row.searched_at = datetime.utcnow() - timedelta(hours=13)
    db.commit()
    forum_cache.search(db, forum, "Hail Mary")
    assert len(forum.calls) == 2
    set_setting(db, "forum_cache_hours", "0")
    forum_cache.search(db, forum, "Hail Mary")
    assert len(forum.calls) == 3


def test_local_index_matches_every_word(db):
    forum_cache.record_topics(db, [{"topic_id": "1", "title": "Book Club - Andy Weir - Project Hail Mary"},
                                   {"topic_id": "2", "title": "Book Club - Andy Weir - The Martian"}])
    assert [m["topic_id"] for m in forum_cache.local_matches(db, "weir martian")] == ["2"]
    assert len(forum_cache.local_matches(db, "andy weir")) == 2
    assert forum_cache.local_matches(db, "ab") == []


def test_saved_post_is_shown_without_asking_the_forum(client, db, settings, monkeypatch):
    settings(abook_url="http://forum/index.php", abook_username="u", abook_password="p")
    forum_cache.save_revealed(db, "1", "11", "Book Club - Andy Weir - Project Hail Mary", REVEALED)
    forum = CountingForum()
    monkeypatch.setattr(search_router, "_get_abook_scraper", lambda db: forum)
    page = client.get("/search/topic/1").text
    assert "abook.ws-1" in page and "saved copy" in page and forum.calls == []
    client.get("/search/topic/1?refresh=1")
    assert forum.calls == [("topic", "1")]


def test_thanking_saves_the_post(client, db, settings, monkeypatch):
    settings(abook_url="http://forum/index.php", abook_username="u", abook_password="p")
    forum = CountingForum()
    monkeypatch.setattr(search_router, "_get_abook_scraper", lambda db: forum)
    client.post("/search/thank", data={"topic_id": "1", "msg_id": "11", "thank_href": "/t", "post_title": "PHM"})
    post = db.query(RevealedPost).one()
    assert post.search_term == "abook.ws-1" and post.password == "pw" and post.topic_id == "1" and post.msg_id == "11"


def test_forum_results_show_cache_note_and_saved_badge(client, db, settings, monkeypatch):
    settings(abook_url="http://forum/index.php", abook_username="u", abook_password="p")
    forum = CountingForum()
    monkeypatch.setattr(search_router, "_get_abook_scraper", lambda db: forum)
    assert "Refresh from forum" not in client.post("/search/forum", data={"query": "Hail Mary"}).text
    forum_cache.save_revealed(db, "1", "11", "x", REVEALED)
    page = client.post("/search/forum", data={"query": "Hail Mary"}).text
    assert "Refresh from forum" in page and "bi-bookmark-star" in page
    assert len(forum.calls) == 1
    assert "Project Hail Mary" in client.get("/search/local", params={"query": "hail mary"}).text


@pytest.fixture
def indexers(monkeypatch, settings):
    settings(nzbget_url="http://nzbget")
    sent = []
    monkeypatch.setattr(nzb_mod, "search_nzbs", lambda source, term: [{"nzb_hash": "h", "title": term, "source": source}])
    monkeypatch.setattr(br, "fetch_nzb", lambda *a: b"<nzb><file/></nzb>")
    monkeypatch.setattr(br, "submit_to_nzbget", lambda db, content, name, password, force=False: sent.append((name, password)) or 77)
    return sent


def test_send_again_from_the_saved_page(client, db, indexers):
    post = forum_cache.save_revealed(db, "1", "11", "Book Club - Andy Weir - Project Hail Mary", REVEALED)
    assert "Project Hail Mary" in client.get("/saved/rows").text
    r = client.post(f"/saved/{post.id}/send").text
    assert "Sent to NZBGet (job 77)" in r and "highlight=" in r
    assert indexers == [("Book Club - Andy Weir - Project Hail Mary", "pw")]
    dl = db.query(Download).one()
    assert dl.search_term == "abook.ws-1" and dl.status == "sent" and dl.parsed_metadata["narrator"] == "Ray Porter"
    db.expire_all()
    assert db.get(RevealedPost, post.id).times_sent == 1


def test_send_again_when_the_nzb_is_gone(client, db, indexers, monkeypatch):
    post = forum_cache.save_revealed(db, "1", "11", "x", REVEALED)
    monkeypatch.setattr(nzb_mod, "search_nzbs", lambda source, term: [])
    r = client.post(f"/saved/{post.id}/send").text
    assert "No NZB found" in r and db.query(Download).count() == 0


def test_saved_page_search_and_forget(client, db):
    forum_cache.save_revealed(db, "1", "11", "x", REVEALED)
    forum_cache.save_revealed(db, "2", "22", "y", dict(REVEALED, search_term="abook.ws-2", title="The Martian"))
    assert "The Martian" not in client.get("/saved/rows", params={"q": "hail"}).text
    assert client.get("/saved").status_code == 200
    martian = db.query(RevealedPost).filter(RevealedPost.title == "The Martian").one()
    client.delete(f"/saved/{martian.id}")
    assert db.query(RevealedPost).count() == 1


def test_request_uses_the_saved_post_instead_of_thanking_again(db, settings, indexers, monkeypatch):
    settings(abook_url="http://forum", abook_username="u", abook_password="p")
    forum = CountingForum()
    monkeypatch.setattr(br, "_scraper", lambda db: forum)
    forum_cache.save_revealed(db, "1", "11", "Book Club - Andy Weir - Project Hail Mary", REVEALED)
    req = br.create_request(db, "Project Hail Mary")
    assert br.run_request(req.id)[0] == "request_sent"
    assert [c[0] for c in forum.calls] == ["search"]   # no topic fetch, no thank


def test_backfill_saves_posts_from_the_download_history(db):
    db.add_all([
        Download(status="downloaded", search_term="abook.ws-9", password="pw9", topic_id="9", msg_id="99",
                 post_title="Book Club - Rob Jones - The Vatican Conspiracy",
                 download_metadata=json.dumps({"title": "The Vatican Conspiracy", "author": "Rob Jones"})),
        Download(status="failed", search_term="abook.ws-9", post_title="retry of the same post"),
        Download(status="sent", search_term=None),
    ])
    db.commit()
    assert forum_cache.backfill_from_downloads(db) == 1
    post = db.query(RevealedPost).one()
    assert post.password == "pw9" and post.title == "The Vatican Conspiracy" and post.topic_id == "9"
    assert forum_cache.backfill_from_downloads(db) == 0   # only once


def test_request_flow_still_saves_freshly_thanked_posts(db, settings, indexers, monkeypatch):
    settings(abook_url="http://forum", abook_username="u", abook_password="p")
    forum = CountingForum(posts={"1": [{"msg_id": "11", "has_hidden": True, "thank_href": "/t", "revealed": None}]})
    monkeypatch.setattr(br, "_scraper", lambda db: forum)
    req = br.create_request(db, "Project Hail Mary")
    br.run_request(req.id)
    db.expire_all()
    assert db.query(RevealedPost).one().search_term == "abook.ws-1"
    assert db.get(BookRequest, req.id).status == "sent"
