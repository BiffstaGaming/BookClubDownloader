"""
Prowlarr: one search across all your usenet (newznab) and torrent (torznab)
indexers. Used when the forum doesn't have a book, from the Search page's
"Indexers" tab, and by requests as a fallback.

Usenet results go to NZBGet, torrent results to qBittorrent (app/services/qbittorrent.py).
"""
import logging
import re

import requests

from app.services.matching import author_similarity, normalize, title_similarity

logger = logging.getLogger(__name__)

AUDIOBOOK_CATEGORY = 3030   # newznab "Audio/Audiobook"
CONFIDENT_SCORE = 0.85      # a request takes a result this good without asking


class ProwlarrError(Exception):
    pass


class ProwlarrClient:
    def __init__(self, url: str, api_key: str):
        self.url = (url or "").rstrip("/")
        self.headers = {"X-Api-Key": api_key or ""}

    def _get(self, path: str, **params):
        resp = requests.get(f"{self.url}{path}", headers=self.headers, params=params, timeout=90)
        if resp.status_code == 401:
            raise ProwlarrError("Prowlarr refused the API key")
        resp.raise_for_status()
        return resp.json()

    def test(self) -> str:
        status = self._get("/api/v1/system/status")
        return f"Prowlarr {status.get('version', '')}".strip()

    def search(self, query: str, limit: int = 100) -> list[dict]:
        data = self._get("/api/v1/search", query=query, type="search", categories=[AUDIOBOOK_CATEGORY], limit=limit)
        return [normalize_result(r) for r in (data if isinstance(data, list) else []) if isinstance(r, dict)]


def normalize_result(raw: dict) -> dict:
    protocol = (raw.get("protocol") or "").lower()
    age_days = raw.get("age")
    if age_days is None and raw.get("ageHours") is not None:
        age_days = raw["ageHours"] / 24
    return {
        "guid": str(raw.get("guid") or raw.get("downloadUrl") or ""),
        "title": raw.get("title") or "",
        "size": int(raw.get("size") or 0),
        "age_days": float(age_days) if age_days is not None else None,
        "indexer": raw.get("indexer") or "",
        "protocol": "torrent" if protocol == "torrent" else "usenet",
        "download_url": raw.get("downloadUrl") or "",
        "magnet_url": raw.get("magnetUrl") or "",
        "info_url": raw.get("infoUrl") or "",
        "seeders": raw.get("seeders"),
    }


def size_label(size_bytes: int) -> str:
    if not size_bytes:
        return ""
    mb = size_bytes / 1024 ** 2
    return f"{mb / 1024:.1f} GB" if mb >= 1024 else f"{mb:.0f} MB"


def _contains_all(needle: str, haystack: str) -> bool:
    words = normalize(needle).split()
    hay = f" {normalize(haystack)} "
    return bool(words) and all(f" {w} " in hay for w in words)


def score_release(title: str, author: str, release_title: str) -> float:
    """0-1: how well a release name ("Andy Weir - Project Hail Mary (2021) [M4B]") matches the book."""
    cleaned = re.sub(r"[\[(][^\])]*[\])]", " ", release_title or "")
    cleaned = re.sub(r"[._]+", " ", cleaned)
    parts = [p.strip() for p in re.split(r"\s+-\s+", cleaned) if p.strip()]
    title_score = max([title_similarity(title, p) for p in parts] + [title_similarity(title, cleaned)])
    if _contains_all(title, cleaned):
        title_score = max(title_score, 0.9)
    if not author:
        return title_score
    author_score = max([author_similarity(author, p) for p in parts] + [0.0])
    if _contains_all(author, cleaned) or _contains_all(author.split()[-1], cleaned):
        author_score = max(author_score, 0.9)
    return title_score * 0.75 + author_score * 0.25


def rank_releases(results: list[dict], title: str, author: str = "", prefer: str = "usenet") -> list[dict]:
    """Best first: closest match, then the preferred protocol, M4B, more seeders, bigger."""
    ranked = []
    for r in results:
        score = round(score_release(title, author, r["title"]), 3)
        ranked.append(dict(r, score=score, size_label=size_label(r["size"])))

    def key(r):
        return (-round(r["score"], 1), r["protocol"] != prefer, "m4b" not in r["title"].lower(),
                -(r.get("seeders") or 0), -r["size"])

    return sorted(ranked, key=key)


def configured(db) -> bool:
    from app.settings_store import get_setting
    return bool(get_setting(db, "prowlarr_url") and get_setting(db, "prowlarr_api_key"))


def client_for(db) -> ProwlarrClient:
    from app.settings_store import get_setting
    return ProwlarrClient(get_setting(db, "prowlarr_url"), get_setting(db, "prowlarr_api_key"))
