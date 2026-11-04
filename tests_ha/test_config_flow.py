"""Config and options flow tests."""

from __future__ import annotations

from unittest.mock import patch

import aiohttp
import pytest

from homeassistant import config_entries
from homeassistant.const import CONF_API_TOKEN, CONF_URL
from homeassistant.core import HomeAssistant
from homeassistant.data_entry_flow import FlowResultType

from custom_components.bookclub.const import (
    CONF_INSTALL_SENTENCES,
    CONF_SCAN_INTERVAL,
    CONF_SEND_EVENTS,
    DOMAIN,
)

from .conftest import STATUS, TOKEN, URL, make_entry


@pytest.fixture
async def mock_setup(hass: HomeAssistant):
    """Skip the real setup (and unload) for flow tests."""
    with (
        patch("custom_components.bookclub.async_setup_entry", return_value=True) as setup,
        patch("custom_components.bookclub.async_unload_entry", return_value=True),
    ):
        yield setup
        for entry in hass.config_entries.async_entries(DOMAIN):
            if entry.state is config_entries.ConfigEntryState.LOADED:
                await hass.config_entries.async_unload(entry.entry_id)


async def _start(hass: HomeAssistant):
    result = await hass.config_entries.flow.async_init(DOMAIN, context={"source": config_entries.SOURCE_USER})
    assert result["type"] is FlowResultType.FORM
    assert result["step_id"] == "user"
    return result


async def test_user_flow_success(hass: HomeAssistant, aioclient_mock, mock_setup) -> None:
    aioclient_mock.get(f"{URL}/api/status", json=STATUS)
    aioclient_mock.get(f"{URL}/api/requests", json=[])
    result = await _start(hass)

    result = await hass.config_entries.flow.async_configure(
        result["flow_id"], {CONF_URL: "bookclub.local:9933/", CONF_API_TOKEN: f" {TOKEN} "}
    )
    await hass.async_block_till_done()

    assert result["type"] is FlowResultType.CREATE_ENTRY
    assert result["title"] == "BookClub"
    assert result["data"] == {CONF_URL: URL, CONF_API_TOKEN: TOKEN}
    assert result["result"].unique_id == URL
    assert len(mock_setup.mock_calls) == 1
    # The token was sent to the protected endpoint.
    method, url, _, headers = aioclient_mock.mock_calls[-1]
    assert url.path == "/api/requests" and url.query["limit"] == "1"
    assert headers["Authorization"] == f"Bearer {TOKEN}"


async def test_user_flow_invalid_auth(hass: HomeAssistant, aioclient_mock) -> None:
    aioclient_mock.get(f"{URL}/api/status", json=STATUS)
    aioclient_mock.get(f"{URL}/api/requests", status=401, json={"detail": "Missing or wrong API token"})
    result = await _start(hass)
    result = await hass.config_entries.flow.async_configure(
        result["flow_id"], {CONF_URL: URL, CONF_API_TOKEN: "wrong"}
    )
    assert result["type"] is FlowResultType.FORM
    assert result["errors"] == {"base": "invalid_auth"}


async def test_user_flow_cannot_connect(hass: HomeAssistant, aioclient_mock) -> None:
    aioclient_mock.get(f"{URL}/api/status", exc=aiohttp.ClientConnectionError())
    result = await _start(hass)
    result = await hass.config_entries.flow.async_configure(
        result["flow_id"], {CONF_URL: URL, CONF_API_TOKEN: TOKEN}
    )
    assert result["type"] is FlowResultType.FORM
    assert result["errors"] == {"base": "cannot_connect"}


async def test_user_flow_not_bookclub(hass: HomeAssistant, aioclient_mock) -> None:
    aioclient_mock.get(f"{URL}/api/status", text="<html>hello</html>")
    result = await _start(hass)
    result = await hass.config_entries.flow.async_configure(
        result["flow_id"], {CONF_URL: URL, CONF_API_TOKEN: TOKEN}
    )
    assert result["errors"] == {"base": "cannot_connect"}


async def test_user_flow_already_configured(hass: HomeAssistant, aioclient_mock) -> None:
    make_entry().add_to_hass(hass)
    result = await _start(hass)
    result = await hass.config_entries.flow.async_configure(
        result["flow_id"], {CONF_URL: URL + "/", CONF_API_TOKEN: TOKEN}
    )
    assert result["type"] is FlowResultType.ABORT
    assert result["reason"] == "already_configured"


async def test_options_flow(hass: HomeAssistant, mock_setup) -> None:
    entry = make_entry()
    entry.add_to_hass(hass)
    result = await hass.config_entries.options.async_init(entry.entry_id)
    assert result["type"] is FlowResultType.FORM
    result = await hass.config_entries.options.async_configure(
        result["flow_id"],
        {CONF_SCAN_INTERVAL: 60, CONF_SEND_EVENTS: True, CONF_INSTALL_SENTENCES: False},
    )
    await hass.async_block_till_done()
    assert result["type"] is FlowResultType.CREATE_ENTRY
    assert entry.options == {CONF_SCAN_INTERVAL: 60, CONF_SEND_EVENTS: True, CONF_INSTALL_SENTENCES: False}


async def test_reauth_flow(hass: HomeAssistant, aioclient_mock, mock_setup) -> None:
    entry = make_entry()
    entry.add_to_hass(hass)
    aioclient_mock.get(f"{URL}/api/requests", json=[])
    result = await hass.config_entries.flow.async_init(
        DOMAIN,
        context={"source": config_entries.SOURCE_REAUTH, "entry_id": entry.entry_id},
        data=entry.data,
    )
    assert result["step_id"] == "reauth_confirm"
    result = await hass.config_entries.flow.async_configure(result["flow_id"], {CONF_API_TOKEN: "new"})
    await hass.async_block_till_done()
    assert result["type"] is FlowResultType.ABORT
    assert result["reason"] == "reauth_successful"
    assert entry.data[CONF_API_TOKEN] == "new"
