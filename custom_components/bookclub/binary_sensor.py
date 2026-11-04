"""BookClub "needs attention" binary sensor."""

from __future__ import annotations

from typing import Any

from homeassistant.components.binary_sensor import (
    BinarySensorDeviceClass,
    BinarySensorEntity,
    BinarySensorEntityDescription,
)
from homeassistant.core import HomeAssistant
from homeassistant.helpers.entity_platform import AddEntitiesCallback

from .coordinator import BookClubConfigEntry
from .entity import BookClubEntity

PARALLEL_UPDATES = 0

ATTENTION_KEYS = ("needs_review", "failed", "requests_waiting")

NEEDS_ATTENTION = BinarySensorEntityDescription(
    key="needs_attention",
    translation_key="needs_attention",
    device_class=BinarySensorDeviceClass.PROBLEM,
)


def _int(value: Any) -> int:
    return value if isinstance(value, int) and not isinstance(value, bool) else 0


async def async_setup_entry(
    hass: HomeAssistant, entry: BookClubConfigEntry, async_add_entities: AddEntitiesCallback
) -> None:
    async_add_entities([BookClubAttention(entry.runtime_data.coordinator, entry, NEEDS_ATTENTION)])


class BookClubAttention(BookClubEntity, BinarySensorEntity):
    """On when a download failed, needs a manual match, or a request waits for a choice."""

    @property
    def is_on(self) -> bool:
        data = self.coordinator.data or {}
        return sum(_int(data.get(key)) for key in ATTENTION_KEYS) > 0

    @property
    def extra_state_attributes(self) -> dict[str, Any]:
        data = self.coordinator.data or {}
        return {key: _int(data.get(key)) for key in ATTENTION_KEYS}
