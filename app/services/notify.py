"""
Home Assistant integration: BookClub POSTs a JSON event to a Home Assistant
webhook URL (an automation with a Webhook trigger) whenever something happens
that a person may want to hear about.

Payload:
  {"event": "book_imported", "title": "...", "author": "...", "download_id": 12,
   "message": "...", "url": "<ABS item link or BookClub link>", "timestamp": "..."}

Events: download_completed, download_failed, needs_review, conversion_failed,
        book_converted, book_imported, abs_import_failed, test
"""
import asyncio
import logging
from datetime import datetime, timezone

import requests

from app.database import SessionLocal
from app.settings_store import get_setting

logger = logging.getLogger(__name__)

EVENTS = {
    "download_completed": "Download finished",
    "download_failed": "Download failed",
    "needs_review": "Needs a manual match",
    "conversion_failed": "Conversion failed",
    "book_converted": "Converted to M4B",
    "book_imported": "Added to Audiobookshelf",
    "abs_import_failed": "Audiobookshelf import failed",
    "test": "Test notification",
}


def build_payload(event: str, title: str = "", author: str = "", download_id: int = None,
                  message: str = "", url: str = "") -> dict:
    return {
        "event": event,
        "event_label": EVENTS.get(event, event),
        "title": title or "",
        "author": author or "",
        "download_id": download_id,
        "message": message or "",
        "url": url or "",
        "timestamp": datetime.now(timezone.utc).isoformat(timespec="seconds"),
    }


def post_webhook(webhook_url: str, payload: dict) -> None:
    resp = requests.post(webhook_url, json=payload, timeout=10)
    resp.raise_for_status()


def send_event_sync(event: str, **fields) -> bool:
    """Send an event if a webhook is configured. Never raises; returns True on success."""
    db = SessionLocal()
    try:
        webhook_url = get_setting(db, "ha_webhook_url")
    finally:
        db.close()
    if not webhook_url:
        return False
    try:
        post_webhook(webhook_url, build_payload(event, **fields))
        return True
    except Exception as exc:
        logger.warning("Home Assistant webhook failed for event %s: %s", event, exc)
        return False


async def send_event(event: str, **fields) -> bool:
    return await asyncio.to_thread(send_event_sync, event, **fields)
