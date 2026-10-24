"""Fetching NZB files from the indexers and submitting them to NZBGet."""
import logging

from sqlalchemy.orm import Session

from app.scrapers.binsearch import BinsearchScraper
from app.scrapers.nzbking import NzbkingScraper
from app.services.nzbget import NzbgetClient
from app.settings_store import get_setting

logger = logging.getLogger(__name__)


class NzbError(Exception):
    pass


def fetch_nzb(source: str, nzb_hash: str, nzb_title: str = "") -> bytes:
    """Download an NZB and make sure it really is one (indexers return HTML error pages)."""
    if source == "binsearch":
        content = BinsearchScraper().download_nzb(nzb_hash, name=nzb_title)
    else:
        content = NzbkingScraper().download_nzb(nzb_hash)
    head = (content or b"")[:4096].lower()
    if b"<nzb" not in head or b"<file" not in (content or b"").lower():
        snippet = head[:120].decode("utf-8", errors="replace").strip()
        raise NzbError(f"{source} did not return a valid NZB file (got: {snippet!r}) — the post may have expired")
    return content


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


def submit_to_nzbget(db: Session, content: bytes, name: str, password: str = "") -> int:
    nzbget_url = get_setting(db, "nzbget_url")
    if not nzbget_url:
        raise NzbError("NZBGet is not configured")
    client = NzbgetClient(nzbget_url, get_setting(db, "nzbget_username"), get_setting(db, "nzbget_password"))
    return client.add_nzb(content, name=name, category=get_setting(db, "nzbget_category"), password=password)
