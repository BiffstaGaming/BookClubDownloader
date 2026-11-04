"""Settings, logs and status behaviour behind the UI fixes."""
from datetime import datetime, timedelta

from app.log_handler import log_to_db
from app.models import Download
from app.settings_store import get_setting


def test_saving_settings_redirects_so_refresh_does_not_resubmit(client, db):
    r = client.post("/settings", data={"poll_interval": "20"}, follow_redirects=False)
    assert r.status_code == 303 and r.headers["location"] == "/settings?saved=1"
    assert "Settings saved successfully" in client.get("/settings?saved=1").text
    assert "Settings saved successfully" not in client.get("/settings").text


def test_event_checkboxes_are_saved(client, db):
    client.post("/settings", data={"ha_events__present": "1", "ha_events": ["book_imported", "request_sent"]})
    db.expire_all()
    assert get_setting(db, "ha_events") == "book_imported,request_sent"
    client.post("/settings", data={"ha_events__present": "1"})
    db.expire_all()
    assert get_setting(db, "ha_events") == "none"


def test_claude_token_is_secret(client, db, settings):
    settings(claude_oauth_token="sk-ant-oat01-secret")
    assert "sk-ant-oat01-secret" not in client.get("/settings").text


def test_regenerate_token(client, db):
    first = client.get("/settings").text.split('id="api_token_view"')[1].split('value="')[1].split('"')[0]
    new = client.post("/settings/regenerate-token").text
    assert first not in new and 'id="api_token_view"' in new


def test_logs_filter_by_download_and_text(client, db):
    log_to_db("INFO", "auto", "#5: needs review — only 70%", download_id=5)
    log_to_db("INFO", "auto", "#6: queued", download_id=6)
    page = client.get("/logs/entries", params={"download": "5"}).text
    assert "only 70%" in page and "#6: queued" not in page
    page = client.get("/logs/entries", params={"search": "queued"}).text
    assert "#6: queued" in page and "only 70%" not in page


def test_times_are_marked_for_local_display(client, db):
    log_to_db("INFO", "x", "hello")
    assert 'class="bc-time"' in client.get("/logs/entries").text


def test_status_counts_imports_by_import_time(client, db):
    old = datetime.utcnow() - timedelta(days=3)
    db.add_all([
        Download(status="downloaded", abs_status="imported", created_at=old, imported_at=datetime.utcnow()),
        Download(status="downloaded", abs_status="imported", created_at=datetime.utcnow(), imported_at=old),
    ])
    db.commit()
    body = client.get("/api/status").json()
    assert body["imported_last_24h"] == 1 and body["last_imported"]


def test_alerts_can_be_closed_and_successes_fade(client, settings):
    from app.ui import alert_html
    assert 'data-bs-dismiss="alert"' in alert_html("danger", "x") and "data-autohide" not in alert_html("danger", "x")
    assert "data-autohide" in alert_html("success", "x")
