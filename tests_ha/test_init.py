"""Setup, sensors, actions, events and voice."""

from __future__ import annotations

from pathlib import Path

import pytest

from homeassistant.config_entries import ConfigEntryState
from homeassistant.core import HomeAssistant
from homeassistant.core_config import async_process_ha_core_config
from homeassistant.exceptions import HomeAssistantError
from homeassistant.helpers import intent
from homeassistant.setup import async_setup_component

from custom_components.bookclub.const import (
    CONF_WEBHOOK_ID,
    DOMAIN,
    EVENT_BOOKCLUB,
    SENTENCES_MARKER,
)
from custom_components.bookclub.voice import status_speech
from pytest_homeassistant_custom_component.common import async_capture_events

from .conftest import STATUS, TOKEN, URL, make_entry


async def _setup(hass: HomeAssistant, entry) -> None:
    entry.add_to_hass(hass)
    assert await hass.config_entries.async_setup(entry.entry_id)
    await hass.async_block_till_done()


async def test_sensors(hass: HomeAssistant, aioclient_mock) -> None:
    aioclient_mock.get(f"{URL}/api/status", json=STATUS)
    aioclient_mock.delete(f"{URL}/api/integration/webhook", json={"url": ""})
    entry = make_entry()
    await _setup(hass, entry)
    assert entry.state is ConfigEntryState.LOADED

    state = hass.states.get("sensor.bookclub_status")
    assert state is not None
    assert state.state == "converting"
    assert state.attributes["queue"] == ["The Martian", "Artemis"]
    assert state.attributes["converting"] == "Project Hail Mary"
    assert state.attributes["converting_progress"] == 42

    assert hass.states.get("sensor.bookclub_downloading").state == "1"
    assert hass.states.get("sensor.bookclub_converting").state == "Project Hail Mary"
    assert hass.states.get("sensor.bookclub_conversion_progress").state == "42"
    assert hass.states.get("sensor.bookclub_last_import").state == "2026-11-04T20:15:00+00:00"
    assert hass.states.get("sensor.bookclub_wishlist_watching").state == "4"

    attention = hass.states.get("binary_sensor.bookclub_needs_attention")
    assert attention.state == "on"
    assert attention.attributes["failed"] == 1

    # Events are off in this entry: BookClub is told to stop sending them.
    assert any(call[0] == "DELETE" for call in aioclient_mock.mock_calls)


async def test_setup_retries_when_unreachable(hass: HomeAssistant, aioclient_mock) -> None:
    aioclient_mock.get(f"{URL}/api/status", status=502, text="bad gateway")
    entry = make_entry()
    entry.add_to_hass(hass)
    await hass.config_entries.async_setup(entry.entry_id)
    assert entry.state is ConfigEntryState.SETUP_RETRY


async def test_request_book_action(hass: HomeAssistant, aioclient_mock) -> None:
    aioclient_mock.get(f"{URL}/api/status", json=STATUS)
    aioclient_mock.delete(f"{URL}/api/integration/webhook", json={"url": ""})
    aioclient_mock.post(
        f"{URL}/api/requests",
        status=202,
        json={"id": 5, "query": "Dune", "status": "pending", "message": "", "options": []},
    )
    await _setup(hass, make_entry())

    response = await hass.services.async_call(
        DOMAIN,
        "request_book",
        {"query": "Dune", "requested_by": "janet"},
        blocking=True,
        return_response=True,
    )
    assert response["id"] == 5
    method, url, body, headers = next(c for c in aioclient_mock.mock_calls if c[0] == "POST")
    assert url.path == "/api/requests"
    assert body == {"query": "Dune", "requested_by": "janet", "force": False}
    assert headers["Authorization"] == f"Bearer {TOKEN}"

    # Without asking for the response the call still works.
    await hass.services.async_call(DOMAIN, "request_book", {"query": "Dune"}, blocking=True)


async def test_action_errors_are_raised(hass: HomeAssistant, aioclient_mock) -> None:
    aioclient_mock.get(f"{URL}/api/status", json=STATUS)
    aioclient_mock.delete(f"{URL}/api/integration/webhook", json={"url": ""})
    aioclient_mock.post(f"{URL}/api/requests/9/choose", status=409, json={"detail": "There is no option 7"})
    await _setup(hass, make_entry())
    with pytest.raises(HomeAssistantError, match="There is no option 7"):
        await hass.services.async_call(
            DOMAIN, "choose_option", {"request_id": 9, "option": 7}, blocking=True
        )


async def test_webhook_fires_event(hass: HomeAssistant, aioclient_mock, hass_client_no_auth) -> None:
    await async_process_ha_core_config(hass, {"internal_url": "http://192.168.1.2:8123"})
    assert await async_setup_component(hass, "http", {})
    aioclient_mock.get(f"{URL}/api/status", json=STATUS)
    aioclient_mock.put(f"{URL}/api/integration/webhook", json={"url": "x"})
    aioclient_mock.delete(f"{URL}/api/integration/webhook", json={"url": ""})
    entry = make_entry(send_events=True)
    await _setup(hass, entry)

    webhook_id = entry.data[CONF_WEBHOOK_ID]
    _, _, body, _ = next(c for c in aioclient_mock.mock_calls if c[0] == "PUT")
    assert body == {"url": f"http://192.168.1.2:8123/api/webhook/{webhook_id}"}

    events = async_capture_events(hass, EVENT_BOOKCLUB)
    status_calls = sum(1 for c in aioclient_mock.mock_calls if c[1].path == "/api/status")
    client = await hass_client_no_auth()
    payload = {"event": "book_imported", "title": "Dune", "author": "Frank Herbert", "request_id": 3}
    resp = await client.post(f"/api/webhook/{webhook_id}", json=payload)
    assert resp.status == 200
    await hass.async_block_till_done()

    assert len(events) == 1
    assert events[0].data["event"] == "book_imported"
    assert events[0].data["title"] == "Dune"
    assert events[0].data["config_entry_id"] == entry.entry_id

    # A bad body is refused and fires nothing.
    resp = await client.post(f"/api/webhook/{webhook_id}", data="not json")
    assert resp.status == 400
    await hass.async_block_till_done()
    assert len(events) == 1

    # Unloading stops the events: webhook gone, BookClub told to forget it.
    assert await hass.config_entries.async_unload(entry.entry_id)
    await hass.async_block_till_done()
    assert [c for c in aioclient_mock.mock_calls if c[0] == "DELETE"]
    resp = await client.post(f"/api/webhook/{webhook_id}", json=payload)
    await hass.async_block_till_done()
    assert len(events) == 1
    assert status_calls >= 1


async def test_voice_intents_and_sentences(
    hass: HomeAssistant, aioclient_mock, tmp_path: Path
) -> None:
    hass.config.config_dir = str(tmp_path)
    aioclient_mock.get(f"{URL}/api/status", json=STATUS)
    aioclient_mock.delete(f"{URL}/api/integration/webhook", json={"url": ""})
    aioclient_mock.post(f"{URL}/api/requests", status=202, json={"id": 1, "status": "pending"})
    entry = make_entry(install_sentences=True)
    await _setup(hass, entry)

    sentences = tmp_path / "custom_sentences" / "en" / "bookclub.yaml"
    text = sentences.read_text(encoding="utf-8")
    assert text.startswith(SENTENCES_MARKER)
    assert "BookClubRequestBook" in text and "wildcard: true" in text

    response = await intent.async_handle(
        hass, "test", "BookClubRequestBook", {"book": {"value": "Project Hail Mary"}}
    )
    assert response.speech["plain"]["speech"] == "OK, I'm looking for Project Hail Mary."
    _, _, body, _ = next(c for c in aioclient_mock.mock_calls if c[0] == "POST")
    assert body["query"] == "Project Hail Mary"
    assert body["requested_by"] == "voice"

    response = await intent.async_handle(hass, "test", "BookClubStatus", {})
    assert response.speech["plain"]["speech"].startswith("BookClub is converting Project Hail Mary")

    # Someone else's file is left alone when the option is turned off.
    sentences.write_text("language: en\n", encoding="utf-8")
    hass.config_entries.async_update_entry(entry, options={**entry.options, "install_sentences": False})
    await hass.async_block_till_done()
    assert sentences.read_text(encoding="utf-8") == "language: en\n"


def test_status_speech() -> None:
    assert status_speech({"state": "idle"}) == "BookClub is idle."
    speech = status_speech(
        {"downloading": 2, "queued": 1, "requests_waiting": 1, "needs_review": 2, "imported_last_24h": 1}
    )
    assert speech == (
        "BookClub is downloading 2 books and 1 book waiting to convert. "
        "1 request is waiting for you to choose a book and 2 downloads need a manual match. "
        "1 book added in the last day."
    )


def test_sentences_parse() -> None:
    """The sentence file is valid for Assist's parser and fills the {book} wildcard."""
    hassil = pytest.importorskip("hassil")
    yaml = pytest.importorskip("yaml")
    from custom_components.bookclub.voice import SENTENCES

    intents = hassil.Intents.from_dict(yaml.safe_load(SENTENCES))
    result = hassil.recognize("download the audiobook Project Hail Mary by Andy Weir", intents)
    assert result is not None
    assert result.intent.name == "BookClubRequestBook"
    assert result.entities["book"].value.strip() == "Project Hail Mary by Andy Weir"

    result = hassil.recognize("what is BookClub doing", intents)
    assert result is not None and result.intent.name == "BookClubStatus"
