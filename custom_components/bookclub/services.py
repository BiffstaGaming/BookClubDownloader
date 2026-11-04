"""BookClub actions (services)."""

from __future__ import annotations

from collections.abc import Awaitable, Callable
from typing import Any

import voluptuous as vol

from homeassistant.config_entries import ConfigEntryState
from homeassistant.core import (
    HomeAssistant,
    ServiceCall,
    ServiceResponse,
    SupportsResponse,
    callback,
)
from homeassistant.exceptions import HomeAssistantError, ServiceValidationError
from homeassistant.helpers import config_validation as cv

from .api import BookClubApi, BookClubAuthError, BookClubError
from .const import DOMAIN, WISHLIST_KINDS
from .coordinator import BookClubConfigEntry

ATTR_CONFIG_ENTRY_ID = "config_entry_id"
ATTR_QUERY = "query"
ATTR_REQUESTED_BY = "requested_by"
ATTR_FORCE = "force"
ATTR_REQUEST_ID = "request_id"
ATTR_OPTION = "option"
ATTR_KIND = "kind"
ATTR_AUTHOR = "author"
ATTR_AUTO_DOWNLOAD = "auto_download"

SERVICE_REQUEST_BOOK = "request_book"
SERVICE_CHOOSE_OPTION = "choose_option"
SERVICE_CANCEL_REQUEST = "cancel_request"
SERVICE_ADD_TO_WISHLIST = "add_to_wishlist"

_ENTRY = {vol.Optional(ATTR_CONFIG_ENTRY_ID): cv.string}

REQUEST_BOOK_SCHEMA = vol.Schema(
    {
        **_ENTRY,
        vol.Required(ATTR_QUERY): vol.All(cv.string, vol.Length(min=2, max=300)),
        vol.Optional(ATTR_REQUESTED_BY): cv.string,
        vol.Optional(ATTR_FORCE, default=False): cv.boolean,
    }
)
CHOOSE_OPTION_SCHEMA = vol.Schema(
    {
        **_ENTRY,
        vol.Required(ATTR_REQUEST_ID): vol.Coerce(int),
        vol.Required(ATTR_OPTION): vol.Coerce(int),
    }
)
CANCEL_REQUEST_SCHEMA = vol.Schema({**_ENTRY, vol.Required(ATTR_REQUEST_ID): vol.Coerce(int)})
ADD_TO_WISHLIST_SCHEMA = vol.Schema(
    {
        **_ENTRY,
        vol.Required(ATTR_QUERY): vol.All(cv.string, vol.Length(min=2, max=200)),
        vol.Optional(ATTR_KIND, default="book"): vol.In(WISHLIST_KINDS),
        vol.Optional(ATTR_AUTHOR, default=""): cv.string,
        vol.Optional(ATTR_AUTO_DOWNLOAD, default=True): cv.boolean,
        vol.Optional(ATTR_REQUESTED_BY): cv.string,
    }
)


def loaded_entries(hass: HomeAssistant) -> list[BookClubConfigEntry]:
    return [
        entry
        for entry in hass.config_entries.async_entries(DOMAIN)
        if entry.state is ConfigEntryState.LOADED
    ]


def entry_for(hass: HomeAssistant, entry_id: str | None) -> BookClubConfigEntry:
    """The entry a call is for: the one named, else the only one loaded."""
    if entry_id:
        entry = hass.config_entries.async_get_entry(entry_id)
        if entry is None or entry.domain != DOMAIN:
            raise ServiceValidationError(f"{entry_id} is not a BookClub config entry")
        if entry.state is not ConfigEntryState.LOADED:
            raise ServiceValidationError("That BookClub server is not loaded")
        return entry
    entries = loaded_entries(hass)
    if not entries:
        raise ServiceValidationError("BookClub is not set up (or not reachable)")
    if len(entries) > 1:
        raise ServiceValidationError("Several BookClub servers are set up: pass config_entry_id")
    return entries[0]


async def user_name(hass: HomeAssistant, user_id: str | None) -> str:
    if not user_id:
        return ""
    user = await hass.auth.async_get_user(user_id)
    return (user.name or "") if user else ""


async def _call_api(call: Callable[[], Awaitable[Any]]) -> Any:
    try:
        return await call()
    except BookClubAuthError as err:
        raise HomeAssistantError(f"BookClub refused the API token: {err}") from err
    except BookClubError as err:
        raise HomeAssistantError(f"BookClub: {err}") from err


@callback
def async_setup_services(hass: HomeAssistant) -> None:
    """Register the BookClub actions (once, for all entries)."""

    async def request_book(call: ServiceCall) -> ServiceResponse:
        entry = entry_for(hass, call.data.get(ATTR_CONFIG_ENTRY_ID))
        api: BookClubApi = entry.runtime_data.api
        requested_by = call.data.get(ATTR_REQUESTED_BY)
        if requested_by is None:
            requested_by = await user_name(hass, call.context.user_id)
        result = await _call_api(
            lambda: api.request_book(call.data[ATTR_QUERY].strip(), requested_by, call.data[ATTR_FORCE])
        )
        await entry.runtime_data.coordinator.async_request_refresh()
        return result if call.return_response else None

    async def choose_option(call: ServiceCall) -> ServiceResponse:
        entry = entry_for(hass, call.data.get(ATTR_CONFIG_ENTRY_ID))
        api: BookClubApi = entry.runtime_data.api
        result = await _call_api(lambda: api.choose(call.data[ATTR_REQUEST_ID], call.data[ATTR_OPTION]))
        await entry.runtime_data.coordinator.async_request_refresh()
        return result if call.return_response else None

    async def cancel_request(call: ServiceCall) -> ServiceResponse:
        entry = entry_for(hass, call.data.get(ATTR_CONFIG_ENTRY_ID))
        api: BookClubApi = entry.runtime_data.api
        result = await _call_api(lambda: api.cancel(call.data[ATTR_REQUEST_ID]))
        await entry.runtime_data.coordinator.async_request_refresh()
        return result if call.return_response else None

    async def add_to_wishlist(call: ServiceCall) -> ServiceResponse:
        entry = entry_for(hass, call.data.get(ATTR_CONFIG_ENTRY_ID))
        api: BookClubApi = entry.runtime_data.api
        requested_by = call.data.get(ATTR_REQUESTED_BY)
        if requested_by is None:
            requested_by = await user_name(hass, call.context.user_id)
        result = await _call_api(
            lambda: api.add_to_wishlist(
                call.data[ATTR_QUERY].strip(),
                call.data[ATTR_KIND],
                call.data[ATTR_AUTHOR],
                call.data[ATTR_AUTO_DOWNLOAD],
                requested_by,
            )
        )
        await entry.runtime_data.coordinator.async_request_refresh()
        return result if call.return_response else None

    for name, handler, schema in (
        (SERVICE_REQUEST_BOOK, request_book, REQUEST_BOOK_SCHEMA),
        (SERVICE_CHOOSE_OPTION, choose_option, CHOOSE_OPTION_SCHEMA),
        (SERVICE_CANCEL_REQUEST, cancel_request, CANCEL_REQUEST_SCHEMA),
        (SERVICE_ADD_TO_WISHLIST, add_to_wishlist, ADD_TO_WISHLIST_SCHEMA),
    ):
        hass.services.async_register(
            DOMAIN, name, handler, schema=schema, supports_response=SupportsResponse.OPTIONAL
        )
