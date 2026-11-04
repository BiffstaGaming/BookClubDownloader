"""The phone UI (/m): redirects, the desktop/phone choice, and phone-sized fragments."""
import json

from app.models import Download
from app.services import forum_cache

PHONE = {"User-Agent": "Mozilla/5.0 (Linux; Android 15; SM-S938B) AppleWebKit/537.36 (KHTML, like Gecko) "
                       "Chrome/131.0 Mobile Safari/537.36"}
DESKTOP = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/131.0 Safari/537.36"}
MOBILE_UI = {"X-BC-UI": "mobile"}


def test_phone_pages_render(client):
    for path in ["/m", "/m/status", "/m/search", "/m/downloads", "/m/saved", "/m/more"]:
        r = client.get(path)
        assert r.status_code == 200, path
    assert 'hx-headers=\'{"X-BC-UI": "mobile"}\'' in client.get("/m").text


def test_phones_are_sent_to_the_phone_ui(client):
    r = client.get("/", headers=PHONE, follow_redirects=False)
    assert r.status_code == 307 and r.headers["location"] == "/m"
    r = client.get("/downloads?highlight=5", headers=PHONE, follow_redirects=False)
    assert r.headers["location"] == "/m/downloads?highlight=5"
    r = client.get("/?q=dune", headers=PHONE, follow_redirects=False)
    assert r.headers["location"] == "/m/search?q=dune"
    assert client.get("/", headers=DESKTOP, follow_redirects=False).status_code == 200
    # Fragments and other pages are never redirected
    assert client.get("/settings", headers=PHONE, follow_redirects=False).status_code == 200
    assert client.get("/downloads", headers={**PHONE, "HX-Request": "true"}, follow_redirects=False).status_code == 200


def test_desktop_view_choice_is_remembered_and_can_be_undone(client):
    r = client.get("/m/desktop?next=/downloads", headers=PHONE, follow_redirects=False)
    assert r.status_code == 303 and r.headers["location"] == "/downloads"
    assert client.get("/downloads", headers=PHONE, follow_redirects=False).status_code == 200
    client.get("/m/phone", headers=PHONE, follow_redirects=False)
    assert client.get("/downloads", headers=PHONE, follow_redirects=False).status_code == 307
    # Only local paths are accepted as `next`
    assert client.get("/m/desktop?next=//evil.example", follow_redirects=False).headers["location"] == "/"


def test_shared_routes_answer_with_phone_fragments(client, db):
    db.add(Download(status="failed", post_title="Broken Book", nzb_hash="h", search_term="t",
                    error_message="Too many missing articles"))
    db.commit()
    desktop = client.post("/downloads/sync-status", data={"dl_filter": "all"}).text
    phone = client.post("/downloads/sync-status", data={"dl_filter": "all"}, headers=MOBILE_UI).text
    assert "<tr" in desktop and 'class="m-card m-dl"' not in desktop
    assert 'class="m-card m-dl"' in phone and "<tr" not in phone
    assert 'hx-target="closest .m-dl"' in phone  # actions replace the card, not a table row

    post = forum_cache.save_revealed(db, "1", "11", "Topic", {"search_term": "abook.ws-1", "title": "Saved Book"})
    assert 'class="m-card m-saved"' in client.get("/saved/rows", headers=MOBILE_UI).text
    assert f'data-saved-id="{post.id}"' in client.get("/saved/rows").text


def test_needs_me_filter(client, db):
    db.add_all([
        Download(status="failed", post_title="Failed One"),
        Download(status="downloaded", post_title="Waiting One", download_metadata=json.dumps({"auto_processed": True})),
        Download(status="sent", post_title="Busy One"),
        Download(status="downloaded", m4b_status="m4b_failed", post_title="Broken Conversion"),
    ])
    db.commit()
    page = client.post("/downloads/sync-status", data={"dl_filter": "attention", "dl_limit": "0"}).text
    assert "Failed One" in page and "Waiting One" in page and "Broken Conversion" in page
    assert "Busy One" not in page


def test_status_cards(client, db):
    db.add(Download(status="sent", post_title="Coming Down", download_progress=42))
    db.commit()
    page = client.get("/m/status").text
    assert "Coming Down" in page and "42%" in page


def test_manifest_and_icons_are_served(client):
    manifest = client.get("/static/manifest.webmanifest")
    assert manifest.status_code == 200 and manifest.json()["start_url"] == "/m"
    assert client.get("/static/icons/icon-192.png").content[:4] == b"\x89PNG"


def test_long_choice_lists_show_more_and_link_to_search(client, db):
    from app.models import BookRequest
    options = [{"option": i, "topic_id": str(i), "title": f"RinaKent- Book {i}"} for i in range(1, 9)]
    db.add(BookRequest(query="Rina kent", status="needs_choice", options=json.dumps(options)))
    db.commit()
    for headers, search_link in (({}, "/?q=Rina%20kent"), (MOBILE_UI, "/m/search?q=Rina%20kent")):
        page = client.get("/downloads/requests", headers=headers).text
        assert "RinaKent- Book 8" in page and "Show 3 more" in page and search_link in page
        assert page.count('hx-vals=\'{"option": ') == 8


def test_request_cards_follow_the_request_through(client, db):
    from datetime import datetime, timedelta
    from app.models import BookRequest
    old = datetime.utcnow() - timedelta(minutes=30)
    db.add_all([
        BookRequest(query="Still Searching", status="searching"),
        BookRequest(query="Just Found", status="sent", download_id=7, updated_at=datetime.utcnow()),
        BookRequest(query="Found Long Ago", status="sent", download_id=6, updated_at=old),
        BookRequest(query="Was Cancelled", status="cancelled"),
        BookRequest(query="No Luck", status="not_found", message="Nothing on the forum matches"),
    ])
    db.commit()
    for headers in ({}, MOBILE_UI):
        page = client.get("/downloads/requests", headers=headers).text
        assert "Still Searching" in page and "Just Found" in page and "No Luck" in page
        assert "Found Long Ago" not in page and "Was Cancelled" not in page
        assert "View download" in page and "Dismiss" in page and "Try again" in page and ">Cancel<" in page
        assert "bi-x-lg" not in page  # no unlabelled X buttons
