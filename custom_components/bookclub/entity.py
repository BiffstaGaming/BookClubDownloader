"""Base entity for BookClub."""

from __future__ import annotations

from homeassistant.helpers.device_registry import DeviceEntryType, DeviceInfo
from homeassistant.helpers.entity import EntityDescription
from homeassistant.helpers.update_coordinator import CoordinatorEntity

from .const import DOMAIN
from .coordinator import BookClubConfigEntry, BookClubCoordinator


class BookClubEntity(CoordinatorEntity[BookClubCoordinator]):
    """An entity on the BookClub device of one config entry."""

    _attr_has_entity_name = True

    def __init__(
        self, coordinator: BookClubCoordinator, entry: BookClubConfigEntry, description: EntityDescription
    ) -> None:
        super().__init__(coordinator)
        self.entity_description = description
        self._attr_unique_id = f"{entry.entry_id}_{description.key}"
        self._attr_device_info = DeviceInfo(
            identifiers={(DOMAIN, entry.entry_id)},
            name="BookClub",
            manufacturer="BookClub",
            model="Audiobook pipeline",
            entry_type=DeviceEntryType.SERVICE,
            configuration_url=coordinator.api.url,
        )
