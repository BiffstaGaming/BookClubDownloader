"""
Home Assistant integration: BookClub POSTs a JSON event to a Home Assistant
webhook URL (an automation with a Webhook trigger) whenever something happens
that a person may want to hear about.

Payload (fields are always present, empty when unknown):
  {"event": "book_imported", "event_label": "...", "title": "...", "author": "...",
   "narrator": "...", "series": "...", "series_part": "...", "cover_url": "...",
   "download_id": 12, "request_id": 3, "requested_by": "janet", "message": "...",
   "url": "<ABS item link, else BookClub link>", "bookclub_url": "...", "abs_url": "...",
   "options": [...], "timestamp": "..."}

Which events are sent is chosen in Settings (all by default).
"""
import asyncio
import logging
import time
from datetime import datetime, timezone

import requests

from app.database import SessionLocal
from app.settings_store import get_setting

logger = logging.getLogger(__name__)

EVENTS = {
    "book_imported": "Added to Audiobookshelf",
    "book_converted": "Converted to M4B",
    "download_completed": "Download finished",
    "download_failed": "Download failed",
    "conversion_failed": "Conversion failed",
    "needs_review": "Needs a manual match",
    "abs_import_failed": "Audiobookshelf import failed",
    "request_sent": "Request found — downloading",
    "request_needs_choice": "Request needs a choice",
    "request_not_found": "Request not found",
    "request_duplicate": "Request already in the library",
    "request_failed": "Request failed",
    "test": "Test notification",
}

RETRY_DELAYS = (2, 5)  # seconds between attempts when Home Assistant doesn't answer


def build_payload(event: str, title: str = "", author: str = "", download_id: int = None,
                  message: str = "", url: str = "", **extra) -> dict:
    payload = {
        "event": event,
        "event_label": EVENTS.get(event, event),
        "title": title or "",
        "author": author or "",
        "narrator": "",
        "series": "",
        "series_part": "",
        "cover_url": "",
        "download_id": download_id,
        "request_id": None,
        "requested_by": "",
        "message": message or "",
        "url": url or "",
        "bookclub_url": "",
        "abs_url": "",
        "options": [],
        "timestamp": datetime.now(timezone.utc).isoformat(timespec="seconds"),
    }
    for key, value in extra.items():
        if value is not None:
            payload[key] = value
    return payload


def post_webhook(webhook_url: str, payload: dict) -> None:
    resp = requests.post(webhook_url, json=payload, timeout=10)
    resp.raise_for_status()


def enabled_events(db) -> set[str]:
    """Events ticked in Settings. Nothing saved yet = all of them."""
    raw = get_setting(db, "ha_events")
    if not raw:
        return set(EVENTS)
    if raw == "none":
        return {"test"}
    return {e for e in raw.split(",") if e} | {"test"}


def bookclub_link(db, download_id: int = None, request_id: int = None) -> str:
    base = get_setting(db, "public_url").rstrip("/")
    if not base:
        return ""
    if download_id:
        return f"{base}/downloads?highlight={download_id}"
    if request_id:
        return f"{base}/downloads#requests"
    return base


def download_fields(dl) -> dict:
    """Notification fields that describe a download row."""
    meta = dl.parsed_metadata
    return {
        "title": meta.get("title") or dl.display_title,
        "author": meta.get("author", ""),
        "narrator": meta.get("narrator", ""),
        "series": meta.get("series", ""),
        "series_part": meta.get("series_part", ""),
        "cover_url": meta.get("cover_url", ""),
        "download_id": dl.id,
        "request_id": dl.request_id,
        "requested_by": dl.requested_by or "",
    }


def send_event_sync(event: str, **fields) -> bool:
    """Send an event if a webhook is configured and the event is wanted. Never raises."""
    try:
        db = SessionLocal()
        try:
            webhook_url = get_setting(db, "ha_webhook_url")
            if not webhook_url or event not in enabled_events(db):
                return False
            link = bookclub_link(db, fields.get("download_id"), fields.get("request_id"))
            fields.setdefault("bookclub_url", link)
            if not fields.get("url"):
                fields["url"] = fields.get("abs_url") or link
        finally:
            db.close()
    except Exception as exc:  # e.g. the database is busy: skip this notification, never fail the caller
        logger.warning("Home Assistant event %s not sent: %s", event, exc)
        return False
    payload = build_payload(event, **fields)
    for attempt, delay in enumerate((*RETRY_DELAYS, None), 1):
        try:
            post_webhook(webhook_url, payload)
            return True
        except Exception as exc:
            if delay is None:
                logger.warning("Home Assistant webhook failed for event %s after %d tries: %s", event, attempt, exc)
                return False
            time.sleep(delay)
    return False


async def send_event(event: str, **fields) -> bool:
    return await asyncio.to_thread(send_event_sync, event, **fields)


def send_for_download(event: str, download_id: int, **fields) -> bool:
    """send_event_sync with the download's own details filled in."""
    from app.models import Download

    try:
        db = SessionLocal()
        try:
            dl = db.query(Download).filter(Download.id == download_id).first()
            base = download_fields(dl) if dl else {"download_id": download_id}
        finally:
            db.close()
    except Exception as exc:
        logger.warning("Home Assistant event %s: could not read download #%s: %s", event, download_id, exc)
        base = {"download_id": download_id}
    return send_event_sync(event, **{**base, **{k: v for k, v in fields.items() if v not in (None, "")}})


async def send_download_event(event: str, download_id: int, **fields) -> bool:
    return await asyncio.to_thread(send_for_download, event, download_id, **fields)
