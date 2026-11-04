"""Constants for the BookClub integration."""

from __future__ import annotations

import logging

DOMAIN = "bookclub"
LOGGER = logging.getLogger(__package__)

CONF_SCAN_INTERVAL = "scan_interval"
CONF_SEND_EVENTS = "send_events"
CONF_INSTALL_SENTENCES = "install_sentences"
CONF_WEBHOOK_ID = "webhook_id"

DEFAULT_SCAN_INTERVAL = 30
MIN_SCAN_INTERVAL = 10
MAX_SCAN_INTERVAL = 3600
DEFAULT_SEND_EVENTS = True
DEFAULT_INSTALL_SENTENCES = True

# Fired on the Home Assistant event bus for every event BookClub sends.
EVENT_BOOKCLUB = "bookclub_event"

# app/services/notify.py EVENTS (kept for documentation / automations).
EVENT_TYPES = (
    "book_imported",
    "book_converted",
    "download_completed",
    "download_failed",
    "download_retrying",
    "conversion_failed",
    "needs_review",
    "abs_import_failed",
    "request_sent",
    "request_needs_choice",
    "request_not_found",
    "request_duplicate",
    "request_failed",
    "wishlist_found",
    "test",
)

# /api/status "state" values.
STATES = ("converting", "downloading", "queued", "searching", "attention", "idle")

WISHLIST_KINDS = ("book", "author", "series")

# Voice
INTENT_REQUEST_BOOK = "BookClubRequestBook"
INTENT_STATUS = "BookClubStatus"
SENTENCES_MARKER = "# Written by the BookClub Home Assistant integration."
