"""Fetching NZB files from the indexers, choosing between them, and submitting them to NZBGet."""
import logging
import re

import requests
from sqlalchemy.orm import Session

from app.scrapers.binsearch import BinsearchScraper
from app.scrapers.nzbking import NzbkingScraper
from app.services.nzbget import NzbgetClient
from app.settings_store import get_setting

logger = logging.getLogger(__name__)

SOURCES = ("nzbking", "binsearch")


class NzbError(Exception):
    pass


def check_nzb(content: bytes, source: str) -> bytes:
    """Make sure a download really is an NZB (indexers return HTML error pages)."""
    head = (content or b"")[:4096].lower()
    if b"<nzb" not in head or b"<file" not in (content or b"").lower():
        snippet = head[:120].decode("utf-8", errors="replace").strip()
        raise NzbError(f"{source} did not return a valid NZB file (got: {snippet!r}) — the post may have expired")
    return content


def fetch_nzb(source: str, nzb_hash: str, nzb_title: str = "", url: str = "") -> bytes:
    """Download an NZB. Prowlarr results are fetched from their own download link (`url`)."""
    if source == "prowlarr":
        if not url:
            raise NzbError("No download link recorded for this Prowlarr result — search again")
        resp = requests.get(url, timeout=60)
        resp.raise_for_status()
        return check_nzb(resp.content, "Prowlarr")
    if source == "binsearch":
        content = BinsearchScraper().download_nzb(nzb_hash, name=nzb_title)
    else:
        content = NzbkingScraper().download_nzb(nzb_hash)
    return check_nzb(content, source)


def search_nzbs(source: str, term: str) -> list[dict]:
    if source == "binsearch":
        results = BinsearchScraper().search(term)
        for r in results:
            r.setdefault("source", "binsearch")
    else:
        # NZBKing matches on the exact subject string — spaces break the query.
        results = NzbkingScraper().search(term.replace(" ", ""))
        for r in results:
            r["source"] = "nzbking"
    return results


# ── choosing the best NZB ─────────────────────────────────────────────────────

_SIZE = re.compile(r"([\d.,]+)\s*([kmgt]?i?b)", re.I)
_SIZE_FACTORS = {"b": 1 / 1024 ** 2, "kb": 1 / 1024, "mb": 1, "gb": 1024, "tb": 1024 ** 2}
_AGE = re.compile(r"([\d.]+)\s*(y|yr|year|mo|month|w|wk|week|d|day|h|hr|hour|m|min|minute)s?\b", re.I)
_AGE_DAYS = {"y": 365, "yr": 365, "year": 365, "mo": 30, "month": 30, "w": 7, "wk": 7, "week": 7, "d": 1, "day": 1,
             "h": 1 / 24, "hr": 1 / 24, "hour": 1 / 24, "m": 1 / 1440, "min": 1 / 1440, "minute": 1 / 1440}


def parse_size_mb(text) -> float | None:
    """'258MB', '1.2 GB', '700 KiB' → megabytes."""
    match = _SIZE.search(str(text or ""))
    if not match:
        return None
    try:
        number = float(match.group(1).replace(",", ""))
    except ValueError:
        return None
    unit = match.group(2).lower().replace("i", "")
    return number * _SIZE_FACTORS.get(unit, 1)


def parse_age_days(text) -> float | None:
    """'26d', '3 hours', '1y' → days."""
    match = _AGE.search(str(text or ""))
    if not match:
        return None
    try:
        return float(match.group(1)) * _AGE_DAYS[match.group(2).lower()]
    except (KeyError, ValueError):
        return None


def rank_nzbs(results: list[dict]) -> list[dict]:
    """
    Best first. Posts of the same book differ mostly in completeness: an incomplete
    post is smaller, an old one is more likely to have expired articles. So: an
    M4B post first (no re-encoding), then the ones within 10% of the largest size,
    newest first, then the rest by size.
    """
    sizes = [s for s in (parse_size_mb(r.get("size")) for r in results) if s]
    largest = max(sizes) if sizes else 0

    def key(result):
        size = parse_size_mb(result.get("size")) or 0
        age = parse_age_days(result.get("age"))
        complete = bool(largest) and size >= largest * 0.9
        m4b = "m4b" in (result.get("title") or "").lower()
        return (not m4b, not complete, age if age is not None else 1e9, -size)

    return sorted(results, key=key)


def find_nzbs(term: str, every_source: bool = False) -> tuple[list[dict], list[str]]:
    """
    (results best first, notes about sources that failed). Asks NZBKing first and
    binsearch only when NZBKing has nothing — unless `every_source`, used when
    looking for a replacement for an NZB that failed.
    """
    results, notes = [], []
    for source in SOURCES:
        try:
            found = search_nzbs(source, term)
        except Exception as exc:
            logger.warning("%s search for '%s' failed: %s", source, term, exc)
            notes.append(f"{source} search failed: {exc}")
            continue
        results += found
        if found and not every_source:
            break
    return rank_nzbs(results), notes


def submit_to_nzbget(db: Session, content: bytes, name: str, password: str = "", force: bool = False) -> int:
    """`force` = a deliberate re-send: NZBGet must not skip it as a duplicate of an earlier download."""
    nzbget_url = get_setting(db, "nzbget_url")
    if not nzbget_url:
        raise NzbError("NZBGet is not configured")
    client = NzbgetClient(nzbget_url, get_setting(db, "nzbget_username"), get_setting(db, "nzbget_password"))
    return client.add_nzb(content, name=name, category=get_setting(db, "nzbget_category"), password=password,
                          dupe_mode="FORCE" if force else "SCORE")
