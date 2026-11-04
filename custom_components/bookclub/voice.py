"""Voice: Assist intents and the optional custom sentence file."""

from __future__ import annotations

from pathlib import Path
from typing import Any

import voluptuous as vol

from homeassistant.core import HomeAssistant
from homeassistant.helpers import config_validation as cv, intent

from .api import BookClubError
from .const import INTENT_REQUEST_BOOK, INTENT_STATUS, LOGGER, SENTENCES_MARKER
from .services import loaded_entries, user_name

SENTENCES_FILE = ("custom_sentences", "en", "bookclub.yaml")

SENTENCES = f"""{SENTENCES_MARKER}
# It is rewritten when the integration starts. To change these sentences, delete
# the first line above: the integration then leaves this file alone.
language: en
intents:
  {INTENT_REQUEST_BOOK}:
    data:
      - sentences:
          - "(find|get|download) [the] [audio]book {{book}}"
          - "add [the] [audio]book {{book}} to [the] library"
  {INTENT_STATUS}:
    data:
      - sentences:
          - "what is bookclub doing"
          - "what's bookclub doing"
          - "[what is the] bookclub status"
lists:
  book:
    wildcard: true
"""


def _plural(count: int, word: str) -> str:
    return f"{count} {word}" if count == 1 else f"{count} {word}s"


def _int(value: Any) -> int:
    return value if isinstance(value, int) and not isinstance(value, bool) else 0


def status_speech(status: dict[str, Any]) -> str:
    """A one-breath spoken summary of /api/status."""
    doing: list[str] = []
    if status.get("converting"):
        text = f"converting {status['converting']}"
        progress = status.get("converting_progress")
        if isinstance(progress, (int, float)) and not isinstance(progress, bool):
            text += f", {round(progress)} percent done"
        doing.append(text)
    if downloading := _int(status.get("downloading")):
        doing.append(f"downloading {_plural(downloading, 'book')}")
    if queued := _int(status.get("queued")):
        doing.append(f"{_plural(queued, 'book')} waiting to convert")
    if searching := _int(status.get("requests_searching")):
        doing.append(f"looking for {_plural(searching, 'requested book')}")

    if not doing:
        sentences = ["BookClub is idle."]
    elif len(doing) == 1:
        sentences = [f"BookClub is {doing[0]}."]
    else:
        sentences = [f"BookClub is {', '.join(doing[:-1])} and {doing[-1]}."]

    attention: list[str] = []
    if waiting := _int(status.get("requests_waiting")):
        verb = "is" if waiting == 1 else "are"
        attention.append(f"{_plural(waiting, 'request')} {verb} waiting for you to choose a book")
    if review := _int(status.get("needs_review")):
        verb = "needs" if review == 1 else "need"
        attention.append(f"{_plural(review, 'download')} {verb} a manual match")
    if failed := _int(status.get("failed")):
        attention.append(f"{_plural(failed, 'item')} failed")
    if attention:
        sentences.append(" and ".join(attention) + ".")

    if imported := _int(status.get("imported_last_24h")):
        sentences.append(f"{_plural(imported, 'book')} added in the last day.")
    return " ".join(sentences)


def _single_entry(hass: HomeAssistant):
    entries = loaded_entries(hass)
    if not entries:
        raise intent.IntentHandleError("BookClub is not set up or not reachable.")
    return entries[0]


class RequestBookIntent(intent.IntentHandler):
    """Find an audiobook and download it."""

    intent_type = INTENT_REQUEST_BOOK
    description = (
        "Asks BookClub to find an audiobook on the forum and download it into the "
        "Audiobookshelf library. Pass the book title, optionally with 'by <author>'."
    )
    slot_schema = {vol.Required("book"): cv.string}

    async def async_handle(self, intent_obj: intent.Intent) -> intent.IntentResponse:
        hass = intent_obj.hass
        slots = self.async_validate_slots(intent_obj.slots)
        book = str(slots["book"]["value"]).strip().strip(".?!")
        if len(book) < 2:
            raise intent.IntentHandleError("Which book?")
        entry = _single_entry(hass)
        requested_by = await user_name(hass, intent_obj.context.user_id) or "voice"
        try:
            await entry.runtime_data.api.request_book(book, requested_by)
        except BookClubError as err:
            LOGGER.warning("Voice request for %s failed: %s", book, err)
            raise intent.IntentHandleError(f"BookClub could not take the request: {err}") from err
        await entry.runtime_data.coordinator.async_request_refresh()
        response = intent_obj.create_response()
        response.async_set_speech(f"OK, I'm looking for {book}.")
        return response


class StatusIntent(intent.IntentHandler):
    """Say what BookClub is doing."""

    intent_type = INTENT_STATUS
    description = (
        "Tells what BookClub (the audiobook downloader) is doing: downloads, conversions, "
        "requests and anything that needs attention."
    )

    async def async_handle(self, intent_obj: intent.Intent) -> intent.IntentResponse:
        entry = _single_entry(intent_obj.hass)
        try:
            status = await entry.runtime_data.api.status()
        except BookClubError as err:
            raise intent.IntentHandleError("I can't reach BookClub right now.") from err
        response = intent_obj.create_response()
        response.async_set_speech(status_speech(status))
        return response


def async_register_intents(hass: HomeAssistant) -> None:
    intent.async_register(hass, RequestBookIntent())
    intent.async_register(hass, StatusIntent())


# ── custom sentences file ──

def _sentences_path(hass: HomeAssistant) -> Path:
    return Path(hass.config.path(*SENTENCES_FILE))


def _is_ours(path: Path) -> bool:
    try:
        with path.open(encoding="utf-8") as fh:
            return fh.readline().strip() == SENTENCES_MARKER
    except OSError:
        return False


def _write_sentences(path: Path) -> bool:
    """Write the file unless someone else's is there. True when it changed."""
    if path.exists():
        if not _is_ours(path):
            LOGGER.info("%s was not written by BookClub; leaving it alone", path)
            return False
        if path.read_text(encoding="utf-8") == SENTENCES:
            return False
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(SENTENCES, encoding="utf-8")
    return True


def _remove_sentences(path: Path) -> bool:
    if path.exists() and _is_ours(path):
        path.unlink()
        return True
    return False


async def _reload_conversation(hass: HomeAssistant) -> None:
    if hass.services.has_service("conversation", "reload"):
        try:
            await hass.services.async_call("conversation", "reload", {}, blocking=False)
        except Exception as err:  # noqa: BLE001 — a failed reload must not break setup
            LOGGER.debug("conversation.reload failed: %s", err)


async def async_install_sentences(hass: HomeAssistant) -> None:
    try:
        changed = await hass.async_add_executor_job(_write_sentences, _sentences_path(hass))
    except OSError as err:
        LOGGER.warning("Could not write the BookClub voice sentences: %s", err)
        return
    if changed:
        await _reload_conversation(hass)


async def async_remove_sentences(hass: HomeAssistant) -> None:
    try:
        changed = await hass.async_add_executor_job(_remove_sentences, _sentences_path(hass))
    except OSError as err:
        LOGGER.warning("Could not remove the BookClub voice sentences: %s", err)
        return
    if changed:
        await _reload_conversation(hass)
