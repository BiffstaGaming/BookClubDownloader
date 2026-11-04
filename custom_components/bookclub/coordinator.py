"""Polls BookClub's /api/status."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import timedelta
import inspect
from typing import Any

from homeassistant.config_entries import ConfigEntry
from homeassistant.core import HomeAssistant
from homeassistant.helpers.update_coordinator import DataUpdateCoordinator, UpdateFailed

from .api import BookClubApi, BookClubError
from .const import CONF_SCAN_INTERVAL, DEFAULT_SCAN_INTERVAL, DOMAIN, LOGGER

# DataUpdateCoordinator only accepts config_entry= from HA 2024.11; older versions take it
# from the setup context instead.
_ACCEPTS_ENTRY = "config_entry" in inspect.signature(DataUpdateCoordinator.__init__).parameters


@dataclass
class BookClubData:
    """What a loaded BookClub entry keeps in entry.runtime_data."""

    api: BookClubApi
    coordinator: BookClubCoordinator
    webhook_id: str | None = None


type BookClubConfigEntry = ConfigEntry[BookClubData]


class BookClubCoordinator(DataUpdateCoordinator[dict[str, Any]]):
    """Fetches the BookClub status summary."""

    def __init__(self, hass: HomeAssistant, entry: ConfigEntry, api: BookClubApi) -> None:
        seconds = entry.options.get(CONF_SCAN_INTERVAL, DEFAULT_SCAN_INTERVAL)
        kwargs: dict[str, Any] = {"config_entry": entry} if _ACCEPTS_ENTRY else {}
        super().__init__(
            hass,
            LOGGER,
            name=DOMAIN,
            update_interval=timedelta(seconds=seconds),
            **kwargs,
        )
        self.api = api

    async def _async_update_data(self) -> dict[str, Any]:
        try:
            return await self.api.status()
        except BookClubError as err:
            raise UpdateFailed(str(err)) from err
