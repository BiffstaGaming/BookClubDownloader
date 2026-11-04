"""'Find and download this book' requests: forum → topic → reveal → NZB → NZBGet."""
import threading

import pytest

from app.models import BookRequest, Download
import re

from app.services import ai, book_requests as br, library


class FakeForum:
    topics = []
    posts = {}
    thanked = []

    def __init__(self):
        self.lock = threading.Lock()

    def search(self, term):
        FakeForum.searches.append(term)
        return FakeForum.topics

    def get_topic(self, topic_id):
        return {"topic_id": topic_id, "title": next(t["title"] for t in FakeForum.topics if t["topic_id"] == topic_id),
                "posts": FakeForum.posts.get(topic_id, [])}

    def thank_and_get_content(self, topic_id, msg_id, href):
        FakeForum.thanked.append(msg_id)
        return {"search_term": "abook.ws-777", "password": "pw", "title": "Project Hail Mary", "author": "Andy Weir"}


HAIL_MARY = {"topic_id": "1", "title": "Book Club - Andy Weir - Project Hail Mary (Ray Porter/2021)"}
MARTIAN = {"topic_id": "2", "title": "Book Club - Andy Weir - The Martian (R.C. Bray)"}


@pytest.fixture
def forum(monkeypatch, settings):
    settings(abook_url="http://forum", abook_username="u", abook_password="p", nzbget_url="http://nzbget",
             ha_webhook_url="http://ha/hook")
    FakeForum.topics, FakeForum.thanked, FakeForum.searches = [HAIL_MARY, MARTIAN], [], []
    FakeForum.posts = {"1": [{"msg_id": "11", "has_hidden": True, "thank_href": "/thank", "revealed": None}]}
    fake = FakeForum()
    monkeypatch.setattr(br, "_scraper", lambda db: fake)
    sent = []
    monkeypatch.setattr(br, "search_nzbs", lambda source, term: [{"nzb_hash": "h1", "title": term, "source": source}])
    monkeypatch.setattr(br, "fetch_nzb", lambda source, h, t: b"<nzb><file/></nzb>")
    monkeypatch.setattr(br, "submit_to_nzbget", lambda db, content, name, password, force=False: sent.append((name, password)) or 55)
    return sent


def test_split_query():
    assert br.split_query("Project Hail Mary by Andy Weir") == ("Project Hail Mary", "Andy Weir")
    assert br.split_query("the audiobook Dune.") == ("Dune", "")


def test_clear_match_is_found_revealed_and_sent(db, forum, clean_db):
    req = br.create_request(db, "Project Hail Mary by Andy Weir", "janet")
    event, fields = br.run_request(req.id)
    db.expire_all()
    req = db.get(BookRequest, req.id)
    assert event == "request_sent" and req.status == "sent", req.message
    assert FakeForum.searches[0] == "Project Hail Mary"     # title only: forum search needs every word
    assert FakeForum.thanked == ["11"] and forum == [(library.sanitize_filename(HAIL_MARY["title"]), "pw")]
    dl = db.get(Download, req.download_id)
    assert dl.requested_by == "janet" and dl.request_id == req.id and dl.nzbget_id == 55
    assert dl.search_term == "abook.ws-777" and dl.parsed_metadata["author"] == "Andy Weir"
    assert fields["requested_by"] == "janet"


def test_unclear_match_asks_and_the_answer_continues(db, forum, settings):
    settings(ai_pick_requests="0")
    FakeForum.topics = [HAIL_MARY, MARTIAN]
    req = br.create_request(db, "Andy Weir")
    event, fields = br.run_request(req.id)
    db.expire_all()
    req = db.get(BookRequest, req.id)
    assert event == "request_needs_choice" and req.status == "needs_choice"
    assert {o["topic_id"] for o in fields["options"]} == {"1", "2"}

    option = next(o["option"] for o in req.parsed_options if o["topic_id"] == "1")
    assert br.choose(db, req, option) == ""
    event, _ = br.run_request(req.id)
    assert event == "request_sent"


def test_claude_picks_when_unclear(db, forum, settings, monkeypatch):
    settings(ai_enabled="1", ai_pick_requests="1", claude_oauth_token="t")
    monkeypatch.setattr(ai, "cli_path", lambda: "claude")

    def claude(prompt, model, token):
        number = re.search(r"#(\d+): .*Project Hail Mary", prompt).group(1)
        return ai.ClaudeResult(text=f'{{"choice": {number}, "confidence": 90, "reason": "space"}}')
    monkeypatch.setattr(ai, "_call", claude)
    req = br.create_request(db, "that Andy Weir space one")
    event, _ = br.run_request(req.id)
    assert event == "request_sent"


def test_duplicate_is_held_unless_forced(db, forum):
    db.add(Download(status="downloaded", search_term="abook.ws-777", post_title="old"))
    db.commit()
    req = br.create_request(db, "Project Hail Mary")
    assert br.run_request(req.id)[0] == "request_duplicate"
    db.expire_all()
    assert br.choose(db, db.get(BookRequest, req.id), force=True) == ""
    assert br.run_request(req.id)[0] == "request_sent"


def test_nothing_on_the_forum(db, forum):
    FakeForum.topics = []
    req = br.create_request(db, "Nonexistent Book")
    assert br.run_request(req.id)[0] == "request_not_found"


def test_cancelled_request_is_not_sent(db, forum, monkeypatch):
    req = br.create_request(db, "Project Hail Mary")

    def cancel_then_search(source, term):
        other = br.SessionLocal()
        other.get(BookRequest, req.id).status = "cancelled"
        other.commit()
        other.close()
        return [{"nzb_hash": "h1", "title": term, "source": source}]
    monkeypatch.setattr(br, "search_nzbs", cancel_then_search)
    assert br.run_request(req.id) is None
    assert forum == []


def test_api_needs_the_token(client, db):
    assert client.post("/api/requests", json={"query": "Dune"}).status_code == 401
    token = client.get("/settings").text.split('id="api_token_view"')[1].split('value="')[1].split('"')[0]
    r = client.post("/api/requests", json={"query": "Dune", "requested_by": "janet"},
                    headers={"Authorization": f"Bearer {token}"})
    assert r.status_code == 202 and r.json()["status"] == "pending" and r.json()["requested_by"] == "janet"
    listed = client.get("/api/requests", headers={"Authorization": f"Bearer {token}"}).json()
    assert listed[0]["query"] == "Dune"
    assert client.post("/api/requests", json={"query": "Dune"}, headers={"Authorization": "Bearer wrong"}).status_code == 401


def test_requests_panel(client, db):
    r = client.post("/downloads/requests", data={"query": "Dune by Frank Herbert"})
    assert "Dune by Frank Herbert" in r.text
    req = db.query(BookRequest).one()
    req.status = "needs_choice"
    req.options = '[{"option": 1, "topic_id": "9", "title": "Book Club - Frank Herbert - Dune"}]'
    db.commit()
    assert "Book Club - Frank Herbert - Dune" in client.get("/downloads/requests").text
    client.delete(f"/downloads/requests/{req.id}")
    db.expire_all()
    assert db.query(BookRequest).count() == 0
