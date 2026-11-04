"""
Book covers kept on BookClub's own disk (data/covers), so pages show them
without fetching them from Audible every time.
"""
import hashlib
import logging
import os
import time

import requests

from app.database import DATA_DIR

logger = logging.getLogger(__name__)

MAX_BYTES = 5 * 1024 * 1024
RETRY_FAILED_AFTER = 24 * 3600  # a cover that couldn't be fetched is tried again a day later


def cover_dir() -> str:
    return os.path.join(DATA_DIR, "covers")


def key(url: str) -> str:
    return hashlib.sha1((url or "").encode("utf-8")).hexdigest()


def cached_path(url: str) -> str | None:
    """The cover on disk, fetching it the first time. None when it can't be had."""
    if not url or not url.startswith(("http://", "https://")):
        return None
    folder = cover_dir()
    path = os.path.join(folder, key(url) + ".img")
    if os.path.isfile(path):
        return path
    missing = path + ".missing"
    if os.path.isfile(missing) and time.time() - os.path.getmtime(missing) < RETRY_FAILED_AFTER:
        return None
    os.makedirs(folder, exist_ok=True)
    try:
        resp = requests.get(url, timeout=15, stream=True)
        resp.raise_for_status()
        if not resp.headers.get("content-type", "").startswith("image/"):
            raise ValueError(f"not an image ({resp.headers.get('content-type')})")
        data = b""
        for chunk in resp.iter_content(64 * 1024):
            data += chunk
            if len(data) > MAX_BYTES:
                raise ValueError("cover is larger than 5 MB")
        tmp = path + ".part"
        with open(tmp, "wb") as f:
            f.write(data)
        os.replace(tmp, path)
        return path
    except Exception as exc:
        logger.info("Cover not cached (%s): %s", url, exc)
        with open(missing, "w", encoding="utf-8") as f:
            f.write(str(exc)[:200])
        return None


def media_type(path: str) -> str:
    with open(path, "rb") as f:
        head = f.read(12)
    if head.startswith(b"\x89PNG"):
        return "image/png"
    if head[:4] == b"RIFF" and head[8:12] == b"WEBP":
        return "image/webp"
    return "image/jpeg"
