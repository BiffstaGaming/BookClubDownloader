"""
Filesystem side of the pipeline: naming, Audiobookshelf folder layout,
metadata.json, covers, and guarded deletion of source folders.
"""
import json
import logging
import os
import re
import shutil
import unicodedata

import requests

logger = logging.getLogger(__name__)

AUDIO_EXTS = {".mp3", ".m4a", ".m4b", ".mp4", ".aac", ".flac", ".ogg", ".oga", ".opus", ".wav", ".wma"}
IMAGE_EXTS = {".jpg", ".jpeg", ".png", ".webp"}

# Audiobookshelf's recommended layout: every book in its own folder, with the
# series sequence first so ABS parses it ("1 - Title").
DEFAULT_LAYOUT = "[Author]/[Series]/[BookNumber] - [Title]/[Title].m4b"


def sanitize_filename(name: str) -> str:
    """Return a filesystem-safe filename/folder component."""
    name = (name or "").replace('\xa0', ' ')
    _unicode_map = {
        '–': '-',   # en dash
        '—': '-',   # em dash
        '‘': "'",   # left single quote
        '’': "'",   # right single quote
        '“': '"',   # left double quote
        '”': '"',   # right double quote
        '…': '...',  # ellipsis
        '·': '.',   # middle dot
        '×': 'x',   # multiplication sign
    }
    for src, dst in _unicode_map.items():
        name = name.replace(src, dst)
    # Strip accents from Latin letters (é → e) but leave other scripts intact
    # (Cyrillic й must not become и), instead of dropping non-ASCII entirely.
    out = []
    for ch in unicodedata.normalize('NFKD', name):
        if unicodedata.combining(ch) and out and out[-1].isascii():
            continue
        out.append(ch)
    name = unicodedata.normalize('NFC', "".join(out))
    name = "".join(ch for ch in name if ch.isprintable())
    # Remove characters Windows/SMB shares forbid in filenames/paths
    name = re.sub(r'[<>:\"/\\|?*]', '_', name)
    name = re.sub(r' +', ' ', name)
    name = name.strip(". ")
    return name[:180] or "download"


def _clean_component(component: str) -> str:
    """Tidy a path component after placeholder substitution (drop dangling separators)."""
    component = re.sub(r'\s+', ' ', component).strip()
    component = re.sub(r'^[-–.\s]+', '', component)      # " - Title" when BookNumber was empty
    component = re.sub(r'[-–\s]+$', '', component)
    component = re.sub(r'\(\s*\)|\[\s*\]|\{\s*\}', '', component).strip()
    return component


def resolve_template(template: str, meta: dict, filename: str = "") -> str:
    """
    Substitute [Placeholders] in a path template. Components that end up empty
    (e.g. [Series] for a standalone book) are dropped.
    Placeholders: [Author] [Title] [Series] [BookNumber] [Narrator] [Year] [Filename]
    """
    title = sanitize_filename(meta.get("title") or "Unknown Title")
    replacements = {
        "[Author]":     sanitize_filename(meta.get("author") or "Unknown Author"),
        "[Title]":      title,
        "[Series]":     sanitize_filename(meta.get("series")) if meta.get("series") else "",
        "[BookNumber]": sanitize_filename(meta.get("series_part")) if meta.get("series") and meta.get("series_part") else "",
        "[Narrator]":   sanitize_filename(meta.get("narrator")) if meta.get("narrator") else "",
        "[Year]":       sanitize_filename(str(meta.get("year"))) if meta.get("year") else "",
        "[Filename]":   filename or f"{title}.m4b",
    }
    absolute = template.startswith("/")
    parts = []
    for raw in re.split(r"[\\/]+", template):
        if not raw:
            continue
        has_placeholder = "[" in raw
        for key, value in replacements.items():
            raw = raw.replace(key, value)
        part = _clean_component(raw) if has_placeholder else raw
        if part:
            parts.append(part)
    path = "/".join(parts)
    if not path.lower().endswith((".m4b", ".m4a", ".mp4")):
        path = f"{path}/{replacements['[Filename]']}"
    return ("/" if absolute else "") + path


def final_book_path(m4b_output_path: str, move_template: str, meta: dict) -> str:
    """Where the finished .m4b should live."""
    if move_template.strip():
        path = resolve_template(move_template.strip(), meta)
        if not path.startswith("/") and m4b_output_path:
            path = os.path.join(m4b_output_path, path)
        return path.replace("\\", "/")
    return os.path.join(m4b_output_path or "/m4b", resolve_template(DEFAULT_LAYOUT, meta)).replace("\\", "/")


def template_layout_warning(move_template: str) -> str:
    """
    Explain why a template is unsafe for Audiobookshelf, or return "".
    ABS needs every book in its own folder; loose files in an author folder can
    be merged into one item.
    """
    template = (move_template or "").strip()
    if not template:
        return ""
    folder = re.split(r"[\\/]", template)[:-1] if template.lower().endswith((".m4b", "[filename]")) else re.split(r"[\\/]", template)
    if not any("[Title]" in part for part in folder):
        return ("Each book needs its own folder for Audiobookshelf — include [Title] in a folder name, "
                f"e.g. /audiobooks/{DEFAULT_LAYOUT}")
    return ""


def find_audio_files(base_path: str) -> list[str]:
    files = []
    for root, dirs, names in os.walk(base_path):
        dirs.sort()
        for name in sorted(names):
            if os.path.splitext(name)[1].lower() in AUDIO_EXTS and not name.startswith("._"):
                files.append(os.path.join(root, name))
    return files


def find_cover_image(base_path: str) -> str:
    """Prefer cover.* / folder.*, otherwise the largest image in the folder tree."""
    best, best_size = "", -1
    for root, _, names in os.walk(base_path):
        for name in names:
            stem, ext = os.path.splitext(name.lower())
            if ext not in IMAGE_EXTS:
                continue
            path = os.path.join(root, name)
            if stem in ("cover", "folder"):
                return path
            size = os.path.getsize(path)
            if size > best_size:
                best, best_size = path, size
    return best


def download_cover(url: str, dest_path: str) -> bool:
    if not url:
        return False
    try:
        resp = requests.get(url, timeout=30)
        resp.raise_for_status()
        if not resp.headers.get("content-type", "image/").startswith("image/") or len(resp.content) < 1000:
            return False
        with open(dest_path, "wb") as f:
            f.write(resp.content)
        return True
    except Exception as exc:
        logger.warning("Cover download failed (%s): %s", url, exc)
        return False


def build_abs_metadata(meta: dict) -> dict:
    """metadata.json in the format Audiobookshelf's scanner reads (ABS 2.2+)."""
    def split_people(value: str) -> list[str]:
        return [p.strip() for p in re.split(r",|;|&", value or "") if p.strip()]

    out = {
        "title": meta.get("title") or None,
        "subtitle": meta.get("subtitle") or None,
        "authors": split_people(meta.get("author")),
        "narrators": split_people(meta.get("narrator")),
        "series": [],
        "genres": [g for g in (meta.get("genres") or []) if g],
        "publishedYear": str(meta["year"]) if meta.get("year") else None,
        "publisher": meta.get("publisher") or None,
        "description": meta.get("description") or None,
        "isbn": meta.get("isbn") or None,
        "asin": meta.get("asin") or None,
        "language": meta.get("language") or None,
        "explicit": False,
        "abridged": False,
        "tags": [],
    }
    if meta.get("series"):
        part = str(meta.get("series_part") or "").replace(" ", "")
        out["series"] = [f"{meta['series']} #{part}" if part else meta["series"]]
    return out


def write_abs_metadata(book_dir: str, meta: dict) -> str:
    path = os.path.join(book_dir, "metadata.json")
    with open(path, "w", encoding="utf-8") as f:
        json.dump(build_abs_metadata(meta), f, indent=2, ensure_ascii=False)
    return path


def _real(path: str) -> str:
    return os.path.normcase(os.path.realpath(path))


def is_within(path: str, root: str) -> bool:
    """True if `path` is strictly inside `root` (not equal to it)."""
    if not path or not root:
        return False
    p, r = _real(path), _real(root)
    return p != r and p.startswith(r.rstrip(os.sep) + os.sep)


def safe_delete_source(input_path: str, download_roots: list[str], protected: list[str]) -> tuple[bool, str]:
    """
    Delete a finished download folder, but only when it is a sub-folder of a
    configured downloads root and does not contain anything we must keep
    (the library, the working folder). Returns (deleted, reason).
    """
    if not input_path or not os.path.isdir(input_path):
        return False, "source folder no longer exists"
    roots = [r for r in download_roots if r and os.path.isdir(r)]
    if not any(is_within(input_path, root) for root in roots):
        return False, f"not inside a downloads folder ({', '.join(roots) or 'none configured'}) — left in place"
    for keep in protected:
        if keep and (_real(keep) == _real(input_path) or is_within(keep, input_path)):
            return False, f"contains {keep} — left in place"
    shutil.rmtree(input_path)
    return True, "deleted"


def map_path(path: str, from_prefix: str, to_prefix: str) -> str:
    """Swap a path prefix (e.g. NZBGet's view of the downloads folder for ours)."""
    if not path or not from_prefix or not to_prefix:
        return path
    norm_from = from_prefix.rstrip("/\\")
    if path == norm_from or path.startswith(norm_from + "/") or path.startswith(norm_from + "\\"):
        remainder = path[len(norm_from):]
        return to_prefix.rstrip("/") + "/" + remainder.lstrip("/\\")
    return path
