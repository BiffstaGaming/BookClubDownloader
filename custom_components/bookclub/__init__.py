"""The BookClub integration: status sensors, actions, events and voice for a BookClub server."""

from __future__ import annotations

from aiohttp import web

from homeassistant.components import webhook
from homeassistant.config_entries import ConfigEntry
from homeassistant.const import CONF_API_TOKEN, CONF_URL, Platform
from homeassistant.core import HomeAssistant
from homeassistant.exceptions import ConfigEntryAuthFailed
from homeassistant.helpers import config_validation as cv
from homeassistant.helpers.aiohttp_client import async_get_clientsession
from homeassistant.helpers.network import NoURLAvailableError, get_url
from homeassistant.helpers.typing import ConfigType

from .api import BookClubApi, BookClubAuthError, BookClubError
from .const import (
    CONF_INSTALL_SENTENCES,
    CONF_SEND_EVENTS,
    CONF_WEBHOOK_ID,
    DEFAULT_INSTALL_SENTENCES,
    DEFAULT_SEND_EVENTS,
    DOMAIN,
    EVENT_BOOKCLUB,
    LOGGER,
)
from .coordinator import BookClubConfigEntry, BookClubCoordinator, BookClubData
from .services import async_setup_services, loaded_entries
from .voice import async_install_sentences, async_register_intents, async_remove_sentences

PLATFORMS: list[Platform] = [Platform.BINARY_SENSOR, Platform.SENSOR]

CONFIG_SCHEMA = cv.config_entry_only_config_schema(DOMAIN)


async def async_setup(hass: HomeAssistant, config: ConfigType) -> bool:
    """Actions and intents exist once, whatever the number of entries."""
    async_setup_services(hass)
    async_register_intents(hass)
    return True


async def async_setup_entry(hass: HomeAssistant, entry: BookClubConfigEntry) -> bool:
    api = BookClubApi(async_get_clientsession(hass), entry.data[CONF_URL], entry.data[CONF_API_TOKEN])
    coordinator = BookClubCoordinator(hass, entry, api)
    await coordinator.async_config_entry_first_refresh()
    entry.runtime_data = BookClubData(api=api, coordinator=coordinator)

    if entry.options.get(CONF_SEND_EVENTS, DEFAULT_SEND_EVENTS):
        await _async_start_events(hass, entry)
    else:
        try:
            await api.clear_webhook()
        except BookClubAuthError as err:
            raise ConfigEntryAuthFailed(str(err)) from err
        except BookClubError as err:
            LOGGER.debug("Could not clear the BookClub webhook: %s", err)

    await hass.config_entries.async_forward_entry_setups(entry, PLATFORMS)

    if entry.options.get(CONF_INSTALL_SENTENCES, DEFAULT_INSTALL_SENTENCES):
        await async_install_sentences(hass)
    elif not _others_want_sentences(hass, entry):
        await async_remove_sentences(hass)

    entry.async_on_unload(entry.add_update_listener(_async_options_updated))
    return True


async def async_unload_entry(hass: HomeAssistant, entry: BookClubConfigEntry) -> bool:
    unloaded = await hass.config_entries.async_unload_platforms(entry, PLATFORMS)
    if unloaded and (webhook_id := entry.runtime_data.webhook_id):
        webhook.async_unregister(hass, webhook_id)
        try:
            await entry.runtime_data.api.clear_webhook()
        except BookClubError as err:
            LOGGER.debug("Could not clear the BookClub webhook: %s", err)
    return unloaded


async def async_remove_entry(hass: HomeAssistant, entry: ConfigEntry) -> None:
    """Take our voice sentences away with the last BookClub entry."""
    if not [e for e in hass.config_entries.async_entries(DOMAIN) if e.entry_id != entry.entry_id]:
        await async_remove_sentences(hass)


async def _async_options_updated(hass: HomeAssistant, entry: BookClubConfigEntry) -> None:
    await hass.config_entries.async_reload(entry.entry_id)


def _others_want_sentences(hass: HomeAssistant, entry: ConfigEntry) -> bool:
    return any(
        e.options.get(CONF_INSTALL_SENTENCES, DEFAULT_INSTALL_SENTENCES)
        for e in loaded_entries(hass)
        if e.entry_id != entry.entry_id
    )


async def _async_start_events(hass: HomeAssistant, entry: BookClubConfigEntry) -> None:
    """Register a webhook here and tell BookClub to send every event to it."""
    data = entry.runtime_data
    webhook_id = entry.data.get(CONF_WEBHOOK_ID)
    if not webhook_id:
        webhook_id = webhook.async_generate_id()
        hass.config_entries.async_update_entry(entry, data={**entry.data, CONF_WEBHOOK_ID: webhook_id})

    try:
        base_url = get_url(hass, allow_external=False, prefer_external=False)
    except NoURLAvailableError:
        LOGGER.warning(
            "BookClub events are off: Home Assistant has no local URL. "
            "Set one under Settings → System → Network"
        )
        return

    async def handle_webhook(hass: HomeAssistant, webhook_id: str, request: web.Request) -> web.Response | None:
        try:
            payload = await request.json()
        except ValueError:
            return web.Response(status=400, text="Expected a JSON body")
        if not isinstance(payload, dict) or not payload.get("event"):
            return web.Response(status=400, text="Expected a BookClub event")
        hass.bus.async_fire(EVENT_BOOKCLUB, {**payload, "config_entry_id": entry.entry_id})
        await data.coordinator.async_request_refresh()
        return None

    webhook.async_register(
        hass,
        DOMAIN,
        "BookClub",
        webhook_id,
        handle_webhook,
        # BookClub runs on the LAN; HA's local_only accepts private/loopback addresses.
        local_only=True,
        allowed_methods=["POST"],
    )
    data.webhook_id = webhook_id

    url = base_url.rstrip("/") + webhook.async_generate_path(webhook_id)
    try:
        await data.api.set_webhook(url)
    except BookClubAuthError as err:
        webhook.async_unregister(hass, webhook_id)
        data.webhook_id = None
        raise ConfigEntryAuthFailed(str(err)) from err
    except BookClubError as err:
        # Keep the webhook: BookClub is reachable for polling, so this is likely transient.
        LOGGER.warning("Could not tell BookClub where to send events (%s): %s", url, err)
    else:
        LOGGER.debug("BookClub sends events to %s", url)
