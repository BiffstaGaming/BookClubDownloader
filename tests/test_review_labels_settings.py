"""The needs-review card says the title is a guess; the new settings save and show."""
import json

from app.models import Download
from app.settings_store import get_setting


def _rows(client, headers=None):
    return client.post("/downloads/sync-status", data={"dl_filter": "all"}, headers=headers or {}).text


def test_needs_review_shows_best_guess_and_review_button(client, db):
    guess = Download(status="downloaded", auto_processed=True, review_reason="Only 72%",
                     download_metadata=json.dumps({"title": "Guessed Title", "match_confidence": 72}))
    mine = Download(status="downloaded", auto_processed=True,
                    download_metadata=json.dumps({"title": "My Title", "match_confidence": 72, "edited_by_hand": True}))
    fresh = Download(status="downloaded", download_metadata=json.dumps({"title": "Still Matching"}))
    db.add_all([guess, mine, fresh])
    db.commit()
    for headers in (None, {"X-BC-UI": "mobile"}):
        page = _rows(client, headers)
        assert page.count("Best guess") == 1 and "Only 72%" in page
        assert page.count("Review &amp; convert") == 2  # both wait for you
        assert ">Convert" in page.replace("\n", "").replace("  ", "")  # the one still being matched


def test_new_settings_save_and_render(client, db):
    form = {"nzb_auto_retry": "3", "prowlarr_url": "http://prowlarr:9696", "prowlarr_api_key": "pk",
            "qbit_url": "http://qbit:8080", "qbit_password": "qp", "encode_window_start": "01:00",
            "encode_window_end": "07:00", "encode_max_temp": "80", "timezone": "Australia/Brisbane",
            "wishlist_check_hours": "6", "audnexus_chapters": "1"}
    assert client.post("/settings", data=form, follow_redirects=False).status_code == 303
    db.expire_all()
    for key, value in form.items():
        assert get_setting(db, key) == value, key
    assert get_setting(db, "prowlarr_requests") == "0"  # unticked box
    page = client.get("/settings").text
    assert 'value="01:00"' in page and "Test Prowlarr" in page and "Test qBittorrent" in page
    assert "pk" not in page and "qp" not in page  # secrets never sent back
    assert "Real chapter names from Audible" in page


def test_new_pages_render_on_both_uis(client):
    for path in ["/wishlist", "/library", "/m/wishlist", "/m/more", "/settings"]:
        assert client.get(path).status_code == 200, path
    assert 'href="/wishlist"' in client.get("/downloads").text
    more = client.get("/m/more").text
    assert 'href="/m/saved"' in more and 'href="/library"' in more


def test_first_loads_wait_for_htmx():
    """
    A request started before htmx has set the page up leaves its element marked
    "loading" for good (htmx resets the element mid-request and its counter goes
    negative), so every spinner inside it spins. Page scripts start them on
    DOMContentLoaded, after htmx's own ready handler.
    """
    from pathlib import Path
    pages = {
        "app/templates/downloads.html": "document.addEventListener('DOMContentLoaded', reloadRows);",
        "app/templates/mobile/downloads.html": "document.addEventListener('DOMContentLoaded', mLoad);",
        "app/templates/index.html": "document.addEventListener('DOMContentLoaded', function restore()",
        "app/templates/mobile/search.html": "document.addEventListener('DOMContentLoaded', function restore()",
        "app/templates/_wishlist_body.html": "document.addEventListener('DOMContentLoaded'",
    }
    for path, expected in pages.items():
        assert expected in Path(path).read_text(encoding="utf-8"), path
