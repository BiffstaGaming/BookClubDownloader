"""
Real chapter names and times from Audnexus (the Audible chapter data Audiobookshelf
also uses), for books whose files only give "Chapter 1, 2, 3" or "Track 01".

Only applied when the book has an ASIN, its chapters look generic, and the Audible
running time matches the file (so the chapter starts land in the right places).
"""
import json
import logging
import os
import re
import subprocess

import requests

logger = logging.getLogger(__name__)

AUDNEXUS_URL = "https://api.audnex.us/books/{asin}/chapters"
# Audible region (Settings → Audible Region) → Audnexus region
REGIONS = {"audible": "us", "audible.uk": "uk", "audible.ca": "ca", "audible.au": "au", "audible.de": "de",
           "audible.fr": "fr", "audible.it": "it", "audible.es": "es", "audible.in": "in", "audible.jp": "jp"}
MATCH_SECONDS = 60       # running times this close are the same recording…
MATCH_FRACTION = 0.005   # …or within half a percent on a long book


def fetch(asin: str, provider: str = "audible") -> dict | None:
    if not re.fullmatch(r"[A-Z0-9]{10}", (asin or "").strip().upper()):
        return None
    try:
        resp = requests.get(AUDNEXUS_URL.format(asin=asin.strip().upper()),
                            params={"region": REGIONS.get(provider, "us")}, timeout=20)
        if resp.status_code != 200:
            return None
        data = resp.json()
    except (requests.RequestException, ValueError) as exc:
        logger.info("Audnexus chapters for %s unavailable: %s", asin, exc)
        return None
    chapters = data.get("chapters") if isinstance(data, dict) else None
    if not chapters or not isinstance(chapters, list):
        return None
    return data


def read_chapters(path: str) -> tuple[list[dict], float]:
    """([{"start", "title"}], duration seconds) of an audio file, via ffprobe."""
    try:
        result = subprocess.run(
            ["ffprobe", "-v", "error", "-show_chapters", "-show_entries", "format=duration", "-of", "json", path],
            capture_output=True, text=True, errors="replace", timeout=120)
        data = json.loads(result.stdout or "{}")
    except Exception:
        return [], 0.0
    chapters = [{"start": float(c.get("start_time") or 0), "title": (c.get("tags") or {}).get("title", "")}
                for c in data.get("chapters") or []]
    try:
        duration = float((data.get("format") or {}).get("duration") or 0)
    except ValueError:
        duration = 0.0
    return chapters, duration


def looks_generic(chapters: list[dict]) -> bool:
    """
    True for chapters that carry no names: none or one, or titles that differ only
    by their numbers ("Chapter 1", "Track 02", "Project Hail Mary - 03").
    """
    if len(chapters) <= 1:
        return True
    stems = {re.sub(r"[\d\W_]+", " ", c.get("title") or "").strip().lower() for c in chapters}
    return len(stems) <= 1


def runtime_matches(audnexus: dict, duration: float) -> bool:
    runtime = (audnexus.get("runtimeLengthMs") or 0) / 1000
    if not runtime or not duration:
        return False
    return abs(runtime - duration) <= max(MATCH_SECONDS, duration * MATCH_FRACTION)


def _timestamp(seconds: float) -> str:
    ms = int(round(seconds * 1000))
    hours, ms = divmod(ms, 3_600_000)
    minutes, ms = divmod(ms, 60_000)
    secs, ms = divmod(ms, 1000)
    return f"{hours:02d}:{minutes:02d}:{secs:02d}.{ms:03d}"


def chapters_file_text(audnexus: dict) -> str:
    """tone's chapter file format: one "HH:MM:SS.mmm Title" line per chapter."""
    lines = []
    for chapter in audnexus.get("chapters") or []:
        start = (chapter.get("startOffsetMs") or 0) / 1000
        title = re.sub(r"\s+", " ", str(chapter.get("title") or "").strip()) or f"Chapter {len(lines) + 1}"
        lines.append(f"{_timestamp(start)} {title}")
    return "\n".join(lines) + "\n"


def apply(m4b_path: str, asin: str, provider: str, work_dir: str, run=subprocess.run) -> str:
    """
    Write Audnexus chapters into the finished book when it makes sense.
    Returns what happened, for the log. Never raises.
    """
    try:
        current, duration = read_chapters(m4b_path)
        if not looks_generic(current):
            return f"kept the book's own {len(current)} named chapters"
        data = fetch(asin, provider)
        if not data:
            return "no Audible chapter list for this ASIN"
        if not runtime_matches(data, duration):
            return (f"Audible's chapters are for a {(data.get('runtimeLengthMs') or 0) / 60000:.0f}-minute recording, "
                    f"this one is {duration / 60:.0f} minutes — kept the existing chapters")
        chapter_file = os.path.join(work_dir, "audnexus-chapters.txt")
        with open(chapter_file, "w", encoding="utf-8") as f:
            f.write(chapters_file_text(data))
        result = run(["tone", "tag", m4b_path, f"--meta-chapters-file={chapter_file}"],
                     capture_output=True, text=True, errors="replace", timeout=600)
        if result.returncode != 0:
            return f"could not write chapters (tone exit {result.returncode}): {(result.stdout or result.stderr or '')[-300:]}"
        return f"named {len(data['chapters'])} chapters from Audible"
    except Exception as exc:  # chapters are a nicety: never fail a conversion over them
        return f"chapter lookup failed: {exc}"
