"""Fixtures for the BookClub Home Assistant integration tests."""

from __future__ import annotations

from typing import Any

import pytest

from homeassistant.const import CONF_API_TOKEN, CONF_URL

from custom_components.bookclub.const import (
    CONF_INSTALL_SENTENCES,
    CONF_SCAN_INTERVAL,
    CONF_SEND_EVENTS,
    DOMAIN,
)
from pytest_homeassistant_custom_component.common import MockConfigEntry

URL = "http://bookclub.local:9933"
TOKEN = "secret-token"

STATUS: dict[str, Any] = {
    "state": "converting",
    "downloading": 1,
    "queued": 2,
    "converting": "Project Hail Mary",
    "converting_progress": 42,
    "needs_review": 0,
    "failed": 1,
    "imported_last_24h": 3,
    "last_converted": "Dune",
    "last_imported": "Dune",
    "last_imported_at": "2026-11-04T20:15:00Z",
    "queue": ["The Martian", "Artemis"],
    "requests_searching": 0,
    "requests_waiting": 0,
    "nzbget_reachable": True,
    "last_poll": "2026-11-05T06:00:00Z",
    "encoding_waiting": None,
    "encoding_note": None,
    "wishlist_watching": 4,
    "wishlist_found": 0,
    "ai_calls_today": 7,
    "ai_plan_5h_percent": 12.5,
}


@pytest.fixture(autouse=True)
def auto_enable_custom_integrations(enable_custom_integrations):
    """Let Home Assistant load custom_components/bookclub."""
    return


@pytest.fixture
def status() -> dict[str, Any]:
    return dict(STATUS)


def make_entry(*, send_events: bool = False, install_sentences: bool = False) -> MockConfigEntry:
    return MockConfigEntry(
        domain=DOMAIN,
        title="BookClub",
        unique_id=URL,
        data={CONF_URL: URL, CONF_API_TOKEN: TOKEN},
        options={
            CONF_SCAN_INTERVAL: 30,
            CONF_SEND_EVENTS: send_events,
            CONF_INSTALL_SENTENCES: install_sentences,
        },
    )
