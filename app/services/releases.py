"""
Sending an indexer (Prowlarr) result: usenet → NZBGet, torrent → qBittorrent.
The download row is then followed by the background poller like any other.
"""
import json
import logging

from app.log_handler import log_to_db
from app.models import Download
from app.services import library, prowlarr
from app.services.nzb import NzbError, fetch_nzb, submit_to_nzbget
from app.services.qbittorrent import QbitClient, QbitError, fetch_torrent, tag_for
from app.settings_store import get_setting

logger = logging.getLogger(__name__)


def qbit_configured(db) -> bool:
    return bool(get_setting(db, "qbit_url"))


def qbit_client(db) -> QbitClient:
    return QbitClient(get_setting(db, "qbit_url"), get_setting(db, "qbit_username"), get_setting(db, "qbit_password"))


def preferred_protocol(db) -> str:
    """Usenet when NZBGet is set up, else torrents."""
    return "usenet" if get_setting(db, "nzbget_url") else "torrent"


def can_send(db, release: dict) -> str:
    """Why this result can't be sent, or ""."""
    if release.get("protocol") == "torrent":
        return "" if qbit_configured(db) else "qBittorrent is not set up in Settings"
    return "" if get_setting(db, "nzbget_url") else "NZBGet is not set up in Settings"


def send_release(db, release: dict, book: dict | None = None, requested_by: str = None,
                 request_id: int = None) -> Download:
    """Raises NzbError / QbitError / requests errors when it can't be sent."""
    book = book or {}
    problem = can_send(db, release)
    if problem:
        raise NzbError(problem) if release.get("protocol") != "torrent" else QbitError(problem)
    name = library.sanitize_filename(release.get("title") or book.get("title") or "download")
    meta = {k: book[k] for k in ("title", "author", "series", "series_part", "narrator") if book.get(k)}
    common = dict(
        post_title=release.get("title") or name, nzb_name=name, nzb_hash=(release.get("guid") or "")[:500] or None,
        nzb_source="prowlarr", nzb_title=release.get("title") or "", indexer=release.get("indexer") or None,
        requested_by=requested_by, request_id=request_id, status="sent",
    )

    if release.get("protocol") != "torrent":
        url = release.get("download_url") or ""
        content = fetch_nzb("prowlarr", release.get("guid", ""), release.get("title", ""), url=url)
        job_id = submit_to_nzbget(db, content, name)
        meta["download_url"] = url
        dl = Download(nzbget_id=job_id, download_metadata=json.dumps(meta, ensure_ascii=False), **common)
        db.add(dl)
        db.commit()
        log_to_db("INFO", "download", f"#{dl.id}: '{dl.post_title}' from {dl.indexer or 'Prowlarr'} sent to NZBGet "
                  f"(job {job_id})", download_id=dl.id)
        return dl

    magnet, torrent = release.get("magnet_url") or "", None
    if not magnet:
        magnet, torrent = fetch_torrent(release.get("download_url") or "")
    dl = Download(client="qbittorrent", download_metadata=json.dumps(meta, ensure_ascii=False) if meta else None,
                  **common)
    db.add(dl)
    db.flush()  # the id names the torrent's tag
    try:
        qbit_client(db).add(tag_for(dl.id), urls=magnet, torrent=torrent, category=get_setting(db, "qbit_category"))
    except Exception:
        db.rollback()
        raise
    db.commit()
    log_to_db("INFO", "download", f"#{dl.id}: '{dl.post_title}' from {dl.indexer or 'Prowlarr'} sent to qBittorrent",
              download_id=dl.id)
    return dl


def search(db, query: str) -> list[dict]:
    """Prowlarr results for a free-text search, best first."""
    from app.services.book_requests import split_query

    title, author = split_query(query)
    results = prowlarr.client_for(db).search(query)
    return prowlarr.rank_releases(results, title, author, prefer=preferred_protocol(db))


def confident_pick(db, ranked: list[dict]) -> dict | None:
    """The best result if it clearly is the book and can be sent."""
    for r in ranked:
        if r["score"] < prowlarr.CONFIDENT_SCORE:
            return None
        if not can_send(db, r):
            return r
    return None
