"""Config and options flow for BookClub."""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any

import voluptuous as vol

from homeassistant.config_entries import ConfigEntry, ConfigFlow, ConfigFlowResult, OptionsFlow
from homeassistant.const import CONF_API_TOKEN, CONF_URL
from homeassistant.core import callback
from homeassistant.helpers.aiohttp_client import async_get_clientsession

from .api import BookClubApi, BookClubAuthError, BookClubConnectionError, BookClubError, normalize_url
from .const import (
    CONF_INSTALL_SENTENCES,
    CONF_SCAN_INTERVAL,
    CONF_SEND_EVENTS,
    DEFAULT_INSTALL_SENTENCES,
    DEFAULT_SCAN_INTERVAL,
    DEFAULT_SEND_EVENTS,
    DOMAIN,
    LOGGER,
    MAX_SCAN_INTERVAL,
    MIN_SCAN_INTERVAL,
)


class BookClubConfigFlow(ConfigFlow, domain=DOMAIN):
    """Ask for the BookClub address and API token."""

    VERSION = 1

    async def async_step_user(self, user_input: dict[str, Any] | None = None) -> ConfigFlowResult:
        errors: dict[str, str] = {}
        if user_input is not None:
            url = normalize_url(user_input[CONF_URL])
            token = user_input[CONF_API_TOKEN].strip()
            await self.async_set_unique_id(url.lower())
            self._abort_if_unique_id_configured()
            api = BookClubApi(async_get_clientsession(self.hass), url, token)
            try:
                await api.status()
                await api.requests(limit=1)
            except BookClubAuthError:
                errors["base"] = "invalid_auth"
            except BookClubConnectionError:
                errors["base"] = "cannot_connect"
            except BookClubError:
                LOGGER.exception("Unexpected answer from BookClub")
                errors["base"] = "unknown"
            else:
                return self.async_create_entry(
                    title="BookClub", data={CONF_URL: url, CONF_API_TOKEN: token}
                )

        defaults = user_input or {}
        schema = vol.Schema(
            {
                vol.Required(CONF_URL, default=defaults.get(CONF_URL, "http://")): str,
                vol.Required(CONF_API_TOKEN, default=defaults.get(CONF_API_TOKEN, "")): str,
            }
        )
        return self.async_show_form(step_id="user", data_schema=schema, errors=errors)

    async def async_step_reauth(self, entry_data: Mapping[str, Any]) -> ConfigFlowResult:
        """BookClub refused the token (e.g. it was changed in BookClub's settings)."""
        return await self.async_step_reauth_confirm()

    async def async_step_reauth_confirm(self, user_input: dict[str, Any] | None = None) -> ConfigFlowResult:
        errors: dict[str, str] = {}
        entry = self.hass.config_entries.async_get_entry(self.context["entry_id"])
        assert entry is not None
        if user_input is not None:
            token = user_input[CONF_API_TOKEN].strip()
            api = BookClubApi(async_get_clientsession(self.hass), entry.data[CONF_URL], token)
            try:
                await api.requests(limit=1)
            except BookClubAuthError:
                errors["base"] = "invalid_auth"
            except BookClubConnectionError:
                errors["base"] = "cannot_connect"
            except BookClubError:
                LOGGER.exception("Unexpected answer from BookClub")
                errors["base"] = "unknown"
            else:
                self.hass.config_entries.async_update_entry(entry, data={**entry.data, CONF_API_TOKEN: token})
                await self.hass.config_entries.async_reload(entry.entry_id)
                return self.async_abort(reason="reauth_successful")
        return self.async_show_form(
            step_id="reauth_confirm",
            data_schema=vol.Schema({vol.Required(CONF_API_TOKEN): str}),
            description_placeholders={"url": entry.data[CONF_URL]},
            errors=errors,
        )

    @staticmethod
    @callback
    def async_get_options_flow(config_entry: ConfigEntry) -> OptionsFlow:
        return BookClubOptionsFlow()


class BookClubOptionsFlow(OptionsFlow):
    """Scan interval, events and voice sentences."""

    async def async_step_init(self, user_input: dict[str, Any] | None = None) -> ConfigFlowResult:
        if user_input is not None:
            return self.async_create_entry(data=user_input)

        # self.handler is the entry id on every HA version (self.config_entry only exists from 2024.11).
        entry = self.hass.config_entries.async_get_entry(self.handler)
        options = entry.options if entry else {}
        schema = vol.Schema(
            {
                vol.Required(
                    CONF_SCAN_INTERVAL, default=options.get(CONF_SCAN_INTERVAL, DEFAULT_SCAN_INTERVAL)
                ): vol.All(vol.Coerce(int), vol.Range(min=MIN_SCAN_INTERVAL, max=MAX_SCAN_INTERVAL)),
                vol.Required(
                    CONF_SEND_EVENTS, default=options.get(CONF_SEND_EVENTS, DEFAULT_SEND_EVENTS)
                ): bool,
                vol.Required(
                    CONF_INSTALL_SENTENCES,
                    default=options.get(CONF_INSTALL_SENTENCES, DEFAULT_INSTALL_SENTENCES),
                ): bool,
            }
        )
        return self.async_show_form(step_id="init", data_schema=schema)
