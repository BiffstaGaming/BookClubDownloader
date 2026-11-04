"""BookClub sensors, one per useful /api/status field."""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from datetime import datetime
from typing import Any

from homeassistant.components.sensor import (
    SensorDeviceClass,
    SensorEntity,
    SensorEntityDescription,
    SensorStateClass,
)
from homeassistant.const import PERCENTAGE, EntityCategory
from homeassistant.core import HomeAssistant
from homeassistant.helpers.entity_platform import AddEntitiesCallback
from homeassistant.util import dt as dt_util

from .const import STATES
from .coordinator import BookClubConfigEntry
from .entity import BookClubEntity

PARALLEL_UPDATES = 0

# Attributes of the main "state" sensor (everything a dashboard card might want in one place).
STATE_ATTRIBUTES = (
    "downloading",
    "queued",
    "converting",
    "converting_progress",
    "queue",
    "needs_review",
    "failed",
    "requests_searching",
    "requests_waiting",
    "last_converted",
    "last_imported",
    "last_imported_at",
    "imported_last_24h",
    "nzbget_reachable",
    "encoding_waiting",
    "encoding_note",
    "wishlist_watching",
    "wishlist_found",
    "last_poll",
)


def _timestamp(value: Any) -> datetime | None:
    if not value:
        return None
    parsed = dt_util.parse_datetime(str(value))
    if parsed is None:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=dt_util.UTC)
    return parsed


def _state(data: dict[str, Any]) -> str | None:
    value = data.get("state")
    return value if value in STATES else None


def _text(key: str) -> Callable[[dict[str, Any]], str | None]:
    def get(data: dict[str, Any]) -> str | None:
        value = data.get(key)
        return str(value)[:255] if value else None

    return get


def _number(key: str) -> Callable[[dict[str, Any]], float | int | None]:
    def get(data: dict[str, Any]) -> float | int | None:
        value = data.get(key)
        if isinstance(value, bool) or not isinstance(value, (int, float)):
            return None
        return value

    return get


@dataclass(frozen=True, kw_only=True)
class BookClubSensorDescription(SensorEntityDescription):
    """A sensor and how to read it from /api/status."""

    value_fn: Callable[[dict[str, Any]], Any]
    attrs_fn: Callable[[dict[str, Any]], dict[str, Any]] | None = None


def _count(key: str, icon: str, *, diagnostic: bool = False) -> BookClubSensorDescription:
    return BookClubSensorDescription(
        key=key,
        translation_key=key,
        icon=icon,
        state_class=SensorStateClass.MEASUREMENT,
        entity_category=EntityCategory.DIAGNOSTIC if diagnostic else None,
        value_fn=_number(key),
    )


SENSORS: tuple[BookClubSensorDescription, ...] = (
    BookClubSensorDescription(
        key="state",
        translation_key="state",
        icon="mdi:book-music",
        device_class=SensorDeviceClass.ENUM,
        options=list(STATES),
        value_fn=_state,
        attrs_fn=lambda data: {key: data.get(key) for key in STATE_ATTRIBUTES},
    ),
    _count("downloading", "mdi:download"),
    _count("queued", "mdi:tray-full"),
    BookClubSensorDescription(
        key="converting",
        translation_key="converting",
        icon="mdi:cog-sync",
        value_fn=_text("converting"),
    ),
    BookClubSensorDescription(
        key="converting_progress",
        translation_key="converting_progress",
        icon="mdi:progress-clock",
        native_unit_of_measurement=PERCENTAGE,
        state_class=SensorStateClass.MEASUREMENT,
        value_fn=_number("converting_progress"),
    ),
    _count("needs_review", "mdi:account-question"),
    _count("failed", "mdi:alert-circle"),
    _count("imported_last_24h", "mdi:bookshelf"),
    BookClubSensorDescription(
        key="last_converted",
        translation_key="last_converted",
        icon="mdi:book-check",
        value_fn=_text("last_converted"),
    ),
    BookClubSensorDescription(
        key="last_imported",
        translation_key="last_imported",
        icon="mdi:book-plus",
        value_fn=_text("last_imported"),
    ),
    BookClubSensorDescription(
        key="last_imported_at",
        translation_key="last_imported_at",
        device_class=SensorDeviceClass.TIMESTAMP,
        value_fn=lambda data: _timestamp(data.get("last_imported_at")),
    ),
    _count("requests_searching", "mdi:book-search"),
    _count("requests_waiting", "mdi:help-circle"),
    _count("wishlist_watching", "mdi:star-outline"),
    _count("wishlist_found", "mdi:star"),
    _count("ai_calls_today", "mdi:robot", diagnostic=True),
    BookClubSensorDescription(
        key="ai_plan_5h_percent",
        translation_key="ai_plan_5h_percent",
        icon="mdi:robot",
        native_unit_of_measurement=PERCENTAGE,
        state_class=SensorStateClass.MEASUREMENT,
        entity_category=EntityCategory.DIAGNOSTIC,
        value_fn=_number("ai_plan_5h_percent"),
    ),
    BookClubSensorDescription(
        key="last_poll",
        translation_key="last_poll",
        device_class=SensorDeviceClass.TIMESTAMP,
        entity_category=EntityCategory.DIAGNOSTIC,
        value_fn=lambda data: _timestamp(data.get("last_poll")),
    ),
)


async def async_setup_entry(
    hass: HomeAssistant, entry: BookClubConfigEntry, async_add_entities: AddEntitiesCallback
) -> None:
    coordinator = entry.runtime_data.coordinator
    async_add_entities(BookClubSensor(coordinator, entry, description) for description in SENSORS)


class BookClubSensor(BookClubEntity, SensorEntity):
    """A value from BookClub's status."""

    entity_description: BookClubSensorDescription

    @property
    def native_value(self) -> Any:
        return self.entity_description.value_fn(self.coordinator.data or {})

    @property
    def extra_state_attributes(self) -> dict[str, Any] | None:
        if self.entity_description.attrs_fn is None:
            return None
        return self.entity_description.attrs_fn(self.coordinator.data or {})
