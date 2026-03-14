import asyncio
import difflib
import json
import logging
import os
import re
import shutil
import unicodedata
from fastapi import APIRouter, BackgroundTasks, Depends, Form, Request
from fastapi.responses import HTMLResponse, Response
from fastapi.templating import Jinja2Templates
from sqlalchemy import and_, or_
from sqlalchemy.orm import Session

from app.database import SessionLocal, get_db
from app.models import Download
from app.scrapers.nzbking import NzbkingScraper
from app.scrapers.binsearch import BinsearchScraper
from app.services.nzbget import NzbgetClient
from app.routers.settings import get_setting
from app.log_handler import log_to_db
from app.services.abs import AbsClient

logger = logging.getLogger(__name__)
router = APIRouter()
templates = Jinja2Templates(directory="app/templates")


def _sanitize_filename(name: str) -> str:
    """Return a Windows-safe filename/folder component with no special characters."""
    # Replace non-breaking and other unicode spaces with regular space
    name = name.replace('\xa0', ' ')
    # Substitute common unicode punctuation with ASCII equivalents before stripping
    _unicode_map = {
        '\u2013': '-',   # en dash
        '\u2014': '-',   # em dash
        '\u2018': "'",   # left single quote
        '\u2019': "'",   # right single quote
        '\u201c': '"',   # left double quote
        '\u201d': '"',   # right double quote
        '\u2026': '...',  # ellipsis
        '\u00b7': '.',   # middle dot
        '\u00d7': 'x',   # multiplication sign
    }
    for src, dst in _unicode_map.items():
        name = name.replace(src, dst)
    # Decompose accented characters (e.g. é → e + combining accent) then drop non-ASCII
    name = unicodedata.normalize('NFKD', name)
    name = name.encode('ascii', 'ignore').decode('ascii')
    # Collapse multiple spaces
    name = re.sub(r' +', ' ', name)
    # Remove characters Windows forbids in filenames/paths
    name = re.sub(r'[<>:\"/\\|?*]', '_', name)
    name = name.strip(". ")
    return name or "download"


def _resolve_move_template(template: str, title: str, author: str, series: str, series_part: str, filename: str) -> str:
    """Substitute [Variable] placeholders in the move path template."""
    replacements = {
        "[Author]":     _sanitize_filename(author) if author else "Unknown Author",
        "[Title]":      _sanitize_filename(title) if title else "Unknown Title",
        "[Series]":     _sanitize_filename(series) if series else "",
        "[BookNumber]": series_part or "",
        "[Filename]":   filename,
    }
    result = template
    for key, value in replacements.items():
        result = result.replace(key, value)
    # Clean up any double slashes or trailing separators from empty substitutions
    result = re.sub(r"[\\/]{2,}", "/", result)
    result = result.strip("/")
    return "/" + result


_AUDIO_EXTS = {".mp3", ".m4a", ".m4b", ".flac", ".wav", ".ogg", ".aac", ".opus"}


def _find_audio_dirs(base_path: str) -> list[str]:
    """
    Walk base_path recursively and return every directory that contains at
    least one audio file, sorted so chapters stay in order.
    Falls back to [base_path] if nothing is found.
    """
    found = []
    for root, dirs, files in os.walk(base_path):
        dirs.sort()  # walk in alphabetical order
        if any(os.path.splitext(f)[1].lower() in _AUDIO_EXTS for f in files):
            found.append(root)
    return found or [base_path]


def _write_abs_metadata(m4b_path: str, title: str, author: str, series: str, series_part: str) -> str:
    """
    Write a metadata.abs file into the same directory as the m4b file.
    Audiobookshelf reads this on scan and it takes full priority over embedded tags.
    Returns the path to the written file.
    """
    meta = {}
    if title:
        meta["title"] = title
    if author:
        meta["author"] = author
    if series:
        meta["series"] = series
        if series_part:
            meta["volumeNumber"] = series_part

    dest_dir = os.path.dirname(m4b_path)
    meta_path = os.path.join(dest_dir, "metadata.abs")
    with open(meta_path, "w", encoding="utf-8") as f:
        json.dump(meta, f, indent=2, ensure_ascii=False)
    return meta_path


AUTO_MATCH_THRESHOLD = 90  # confidence % required for automatic conversion


def _parse_nzb_name(name: str) -> tuple[str, str]:
    """
    Parse the forum NZB naming convention:
      "Book Club - [FLAG] Author - Series## - BookTitle (Narrator/Year)"
    Returns (title, author).
    Strips any trailing parenthetical (year OR narrator name).
    Strips bracketed flags like [SPOT] from the author segment.
    """
    # Strip any trailing (...) — covers both "(2025)" and "(Colin Mace)"
    name = re.sub(r'\s*\([^)]+\)\s*$', '', name).strip()
    parts = [p.strip() for p in name.split(' - ')]
    # Need at least "Book Club - Author - Title"
    if len(parts) >= 3:
        title  = parts[-1]
        author = re.sub(r'^\[[^\]]+\]\s*', '', parts[1]).strip()  # strip [SPOT] etc.
    elif len(parts) == 2:
        title  = parts[-1]
        author = ""
    else:
        title  = name
        author = ""
    return title, author


def _extract_nzb_title(name: str) -> str:
    return _parse_nzb_name(name)[0]


def _extract_nzb_author(name: str) -> str:
    return _parse_nzb_name(name)[1]


def _extract_audible_series(book: dict) -> tuple[str, str]:
    """Return (series_name, series_part) from an ABS/Audible search result dict."""
    series_raw = book.get("series")
    if isinstance(series_raw, list) and series_raw:
        s = series_raw[0]
        name = s.get("name") or s.get("series") or ""
        part = str(s.get("position") or s.get("volumeNumber") or s.get("sequence") or "")
    elif isinstance(series_raw, str):
        name = series_raw
        part = ""
        for k in ("volumeNumber", "sequence", "position", "seriesSequence"):
            v = book.get(k)
            if v:
                part = str(v)
                break
    else:
        name = ""
        part = ""
    return name, part


def _compute_confidence(nzb_title: str, nzb_author: str, audible_title: str, audible_author: str) -> int:
    """
    Return 0-100 match confidence.
    Title is the decisive factor — if it scores < 50%, author match cannot save it.
    Above 50% title similarity, author (35%) is factored in.
    """
    def _norm(s: str) -> str:
        return re.sub(r'[^\w\s]', '', (s or "").lower()).strip()

    def _ratio(a: str, b: str) -> float:
        a, b = _norm(a), _norm(b)
        if not a or not b:
            return 0.0
        return difflib.SequenceMatcher(None, a, b).ratio()

    title_score = _ratio(nzb_title, audible_title)
    # If the title doesn't match well enough, don't let author inflate the score
    if title_score < 0.5:
        return round(title_score * 100)
    if nzb_author and audible_author:
        author_score = _ratio(nzb_author, audible_author)
        score = title_score * 0.65 + author_score * 0.35
    else:
        score = title_score
    return round(score * 100)


def _best_audible_match(results: list, nzb_title: str) -> dict:
    """
    From up to 5 Audible results, return the one whose title best matches
    the extracted NZB title — rather than blindly taking results[0].
    Audible sometimes returns the most-popular book in a series first even
    when a different book in that series was searched.
    """
    def _title_ratio(book: dict) -> float:
        a = re.sub(r'[^\w\s]', '', (nzb_title or "").lower()).strip()
        b = re.sub(r'[^\w\s]', '', (book.get("title") or "").lower()).strip()
        if not a or not b:
            return 0.0
        return difflib.SequenceMatcher(None, a, b).ratio()

    candidates = results[:5]
    return max(candidates, key=_title_ratio)


async def _auto_process_download(download_id: int):
    """
    Background task triggered when a download completes.
    Fetches Audible metadata, computes confidence, and auto-converts if >= AUTO_MATCH_THRESHOLD.
    """
    db = SessionLocal()
    try:
        dl = db.query(Download).filter(Download.id == download_id).first()
        if not dl or dl.status != "downloaded":
            return

        saved = {}
        if dl.download_metadata:
            try:
                saved = json.loads(dl.download_metadata)
            except Exception:
                pass

        # Skip if already processed (e.g. re-poll after manual fetch)
        if "match_confidence" in saved:
            return

        abs_url    = get_setting(db, "abs_url")
        abs_token  = get_setting(db, "abs_token")
        m4b_output = get_setting(db, "m4b_output_path")

        nzb_name   = dl.post_title or dl.nzb_name or ""
        nzb_title, nzb_author_from_name = _parse_nzb_name(nzb_name)
        # NZB filename is the most reliable source — forum metadata can be wrong.
        # Ignore saved author if it looks like a series designation (ends with a digit).
        saved_author = saved.get("author", "")
        if saved_author and re.search(r'\d$', saved_author.strip()):
            saved_author = ""  # looks like "Series Name 1", not a real author name
        nzb_author = nzb_author_from_name or saved_author

        if not abs_url or not abs_token:
            log_to_db("INFO", "auto", f"Download #{download_id} complete — ABS not configured, skipping auto-match", download_id=download_id)
            return
        if not nzb_title:
            log_to_db("WARNING", "auto", f"Download #{download_id}: could not extract title from '{nzb_name}'", download_id=download_id)
            return

        log_to_db("INFO", "auto",
            f"Download #{download_id}: searching Audible — title='{nzb_title}' author='{nzb_author}' (from: '{nzb_name}')",
            download_id=download_id)

        try:
            client  = AbsClient(abs_url, abs_token)
            results = await asyncio.to_thread(client.search_books, nzb_title.strip(), nzb_author.strip())

            if not results:
                saved["match_confidence"] = 0
                dl.download_metadata = json.dumps(saved, ensure_ascii=False)
                db.commit()
                log_to_db("WARNING", "auto", f"Download #{download_id}: no Audible results for title='{nzb_title}' author='{nzb_author}'", download_id=download_id)
                return

            candidate_titles = [r.get("title", "") for r in results[:5]]
            log_to_db("DEBUG", "auto",
                f"Download #{download_id}: top {len(candidate_titles)} Audible candidates: {candidate_titles}",
                download_id=download_id)

            book           = _best_audible_match(results, nzb_title)
            audible_title  = book.get("title", "")
            audible_author = book.get("author") or book.get("authors") or ""
            audible_series, audible_series_part = _extract_audible_series(book)
            confidence     = _compute_confidence(nzb_title, nzb_author, audible_title, audible_author)

            log_to_db("INFO", "auto",
                f"Download #{download_id}: best match '{audible_title}' by '{audible_author}' — confidence {confidence}%",
                download_id=download_id)

            saved["title"]            = audible_title       or saved.get("title", "")
            saved["author"]           = audible_author      or saved.get("author", "")
            saved["series"]           = audible_series      or saved.get("series", "")
            saved["series_part"]      = audible_series_part or saved.get("series_part", "")
            saved["match_confidence"] = confidence

            if confidence >= AUTO_MATCH_THRESHOLD and dl.download_path:
                safe_title  = _sanitize_filename(saved["title"] or nzb_title)
                output_file = (
                    os.path.join(m4b_output, f"{safe_title}.m4b").replace("\\", "/")
                    if m4b_output else f"/m4b/{safe_title}.m4b"
                )
                saved["input_path"]  = dl.download_path
                saved["output_file"] = output_file
                dl.download_metadata = json.dumps(saved, ensure_ascii=False)
                dl.m4b_status        = "converting"
                dl.m4b_progress      = None
                db.commit()
                db.close()
                db = None

                log_to_db("INFO", "auto",
                    f"Download #{download_id}: auto-converting — confidence {confidence}% ≥ {AUTO_MATCH_THRESHOLD}%",
                    download_id=download_id)
                await _run_m4b_conversion(
                    download_id, dl.download_path, output_file,
                    saved["title"], saved["author"],
                    saved.get("series", ""), saved.get("series_part", ""),
                )
            else:
                dl.download_metadata = json.dumps(saved, ensure_ascii=False)
                db.commit()
                if not dl.download_path:
                    log_to_db("WARNING", "auto", f"Download #{download_id}: no download path — cannot auto-convert", download_id=download_id)
                else:
                    log_to_db("INFO", "auto",
                        f"Download #{download_id}: confidence {confidence}% below {AUTO_MATCH_THRESHOLD}% — manual review needed",
                        download_id=download_id)

        except Exception as exc:
            logger.error("_auto_process_download failed for #%d: %s", download_id, exc)
            log_to_db("ERROR", "auto", f"Auto-match failed for #{download_id}: {exc}", download_id=download_id)
    finally:
        if db:
            db.close()


def _map_path(path: str, nzbget_prefix: str, local_prefix: str) -> str:
    """Replace the nzbget path prefix with the local path prefix."""
    if nzbget_prefix and local_prefix and path.startswith(nzbget_prefix):
        remainder = path[len(nzbget_prefix):]
        # Ensure exactly one slash between the local prefix and the remainder,
        # regardless of whether the saved prefixes have trailing/leading slashes.
        return local_prefix.rstrip("/") + "/" + remainder.lstrip("/")
    return path


async def _abs_scan_and_match(download_id: int, title: str, author: str, abs_url: str, abs_token: str, abs_library_id: str):
    """After conversion, scan ABS library and trigger Quick Match on the new item."""
    if not abs_url or not abs_token or not abs_library_id:
        return

    client = AbsClient(abs_url, abs_token)

    # Trigger a library scan so ABS picks up the new file
    try:
        client.scan_library(abs_library_id)
        log_to_db("INFO", "abs", "ABS library scan triggered", download_id=download_id)
    except Exception as exc:
        log_to_db("WARNING", "abs", f"ABS scan trigger failed: {exc}", download_id=download_id)

    # Poll until the item appears in search (up to ~3 minutes)
    item_id = None
    for attempt in range(18):  # 18 × 10s = 3 min
        await asyncio.sleep(10)
        try:
            results = client.search_library(abs_library_id, title)
            if results:
                item_id = results[0]["id"]
                log_to_db("INFO", "abs", f"Book found in ABS (item {item_id}) after {(attempt + 1) * 10}s", download_id=download_id)
                break
        except Exception as exc:
            log_to_db("DEBUG", "abs", f"ABS search attempt {attempt + 1} failed: {exc}", download_id=download_id)

    if not item_id:
        log_to_db("WARNING", "abs", "Book not found in ABS after 3 minutes — Quick Match skipped", download_id=download_id)
        return

    try:
        result = client.quick_match(item_id, title, author)
        updated = result.get("updated", False)
        if updated:
            log_to_db("INFO", "abs", f"Quick Match applied successfully for ABS item {item_id}", download_id=download_id)
        else:
            log_to_db("WARNING", "abs", f"Quick Match found no update for ABS item {item_id} — response: {result}", download_id=download_id)
    except Exception as exc:
        log_to_db("ERROR", "abs", f"Quick Match failed for item {item_id}: {exc}", download_id=download_id)


async def _run_m4b_conversion(
    download_id: int,
    input_path: str,
    output_file: str,
    title: str,
    author: str,
    series: str,
    series_part: str,
):
    """Background task: run m4b-tool, optionally move the result, then update the DB."""
    db = SessionLocal()
    try:
        move_template = get_setting(db, "m4b_move_template")
        m4b_jobs = get_setting(db, "m4b_jobs")
        m4b_bitrate = get_setting(db, "m4b_bitrate")

        # ── Pre-flight check ──────────────────────────────────────────────────
        # If there is already an .m4b in the input folder, use it directly
        # rather than re-encoding with m4b-tool.
        existing_m4bs = []
        for root, _, files in os.walk(input_path):
            for f in files:
                if f.lower().endswith(".m4b"):
                    existing_m4bs.append(os.path.join(root, f))

        if existing_m4bs:
            existing = existing_m4bs[0]
            log_to_db("INFO", "conversion",
                      f"Existing .m4b found — skipping m4b-tool: {existing}",
                      download_id=download_id)
            log = f"Existing .m4b used: {existing}"
            returncode = 0
            _no_files = False
            # Treat the existing file as the conversion output
            output_file = existing
        else:
            # Check there are actually MP3s to convert before invoking m4b-tool
            audio_dirs = _find_audio_dirs(input_path)
            has_mp3s = any(
                f.lower().endswith(".mp3")
                for d in audio_dirs
                for f in os.listdir(d)
            )
            if not has_mp3s:
                log_to_db("ERROR", "conversion",
                          "No .mp3 or .m4b files found in input path — nothing to convert",
                          download_id=download_id)
                dl = db.query(Download).filter(Download.id == download_id).first()
                if dl:
                    dl.m4b_status = "m4b_failed"
                    dl.conversion_log = "No .mp3 or .m4b files found in input path."
                    db.commit()
                return

            log_to_db("DEBUG", "conversion",
                      f"Audio directories found: {audio_dirs}", download_id=download_id)

            cmd = [
                "m4b-tool", "merge", *audio_dirs,
                f"--output-file={output_file}",
                "--no-interaction",
            ]
            if title:
                cmd += [f"--name={title}", f"--album={title}"]
            if author:
                cmd += [f"--artist={author}", f"--albumartist={author}"]
            if series:
                cmd.append(f"--series={series}")
            if series_part:
                cmd.append(f"--series-part={series_part}")
            if m4b_jobs:
                cmd.append(f"--jobs={m4b_jobs}")
            if m4b_bitrate:
                cmd.append(f"--audio-bitrate={m4b_bitrate}")

            logger.info("Starting m4b-tool for download #%d: %s", download_id, " ".join(cmd))
            log_to_db("INFO", "conversion", "Starting m4b-tool conversion", download_id=download_id)

            proc = await asyncio.create_subprocess_exec(
                *cmd,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.STDOUT,
            )

            # Stream stdout in chunks and split on \r OR \n so we catch carriage-return
            # progress bars (m4b-tool uses \r to overwrite the current line in-place).
            buf = ""
            log_lines = []
            last_progress = -1

            while True:
                chunk = await proc.stdout.read(512)
                if not chunk:
                    break
                buf += chunk.decode("utf-8", errors="replace")
                # Split on \r\n, \n, or bare \r — keep the last incomplete fragment
                parts = re.split(r'\r\n|\n|\r', buf)
                buf = parts[-1]
                for line in parts[:-1]:
                    if line.strip():
                        log_lines.append(line)
                    m = re.search(r'\b(\d{1,3})%', line)
                    if m:
                        pct = min(100, int(m.group(1)))
                        if pct != last_progress:
                            last_progress = pct
                            try:
                                dl_prog = db.query(Download).filter(Download.id == download_id).first()
                                if dl_prog:
                                    dl_prog.m4b_progress = pct
                                    db.commit()
                            except Exception:
                                pass

                # Also check the incomplete buffer fragment for progress updates
                m = re.search(r'\b(\d{1,3})%', buf)
                if m:
                    pct = min(100, int(m.group(1)))
                    if pct != last_progress:
                        last_progress = pct
                        try:
                            dl_prog = db.query(Download).filter(Download.id == download_id).first()
                            if dl_prog:
                                dl_prog.m4b_progress = pct
                                db.commit()
                        except Exception:
                            pass

            if buf.strip():
                log_lines.append(buf)

            await proc.wait()
            log = "\n".join(log_lines)
            returncode = proc.returncode

            # Store the full m4b-tool output as a DEBUG entry linked to this download
            log_to_db("DEBUG", "conversion", f"m4b-tool output:\n{log}", download_id=download_id)

            # m4b-tool sometimes exits 0 even on soft failures — check output too
            _no_files = "no files to convert" in log.lower()

        # ── Common success / failure path ─────────────────────────────────────
        dl = db.query(Download).filter(Download.id == download_id).first()
        if dl:
            if returncode == 0 and not _no_files:
                final_path = output_file
                # Move the file if a move template is configured
                if move_template and os.path.isfile(output_file):
                    filename = os.path.basename(output_file)
                    dest = _resolve_move_template(
                        move_template, title, author, series, series_part, filename
                    )
                    try:
                        os.makedirs(os.path.dirname(dest), exist_ok=True)
                        shutil.move(output_file, dest)
                        final_path = dest
                        logger.info("Moved M4B to %s", dest)
                        # Clean up the input folder now that the m4b is safely moved
                        if (os.path.isdir(input_path) and
                                os.path.abspath(input_path) != os.path.abspath(os.path.dirname(dest))):
                            try:
                                shutil.rmtree(input_path)
                                log_to_db("INFO", "conversion", f"Deleted input folder: {input_path}", download_id=download_id)
                            except Exception as rm_exc:
                                log_to_db("WARNING", "conversion", f"Could not delete input folder {input_path}: {rm_exc}", download_id=download_id)
                    except Exception as move_exc:
                        logger.warning("Could not move M4B to %s: %s", dest, move_exc)
                        log += f"\n[Move failed: {move_exc}]"
                        log_to_db("ERROR", "conversion", f"Move failed to {dest}: {move_exc}", download_id=download_id)
                        dl.m4b_status = "move_failed"
                        dl.m4b_path = output_file  # file still exists here
                        dl.conversion_log = log[-4000:]
                        db.commit()
                        return
                dl.m4b_status = "converted"
                dl.m4b_path = final_path
                logger.info("M4B conversion complete for download #%d: %s", download_id, final_path)
                log_to_db("INFO", "conversion", f"M4B ready at: {final_path}", download_id=download_id)
                # Write metadata.abs for Audiobookshelf
                try:
                    meta_path = _write_abs_metadata(final_path, title, author, series, series_part)
                    log_to_db("INFO", "conversion", f"metadata.abs written at: {meta_path}", download_id=download_id)
                except Exception as meta_exc:
                    logger.warning("Could not write metadata.abs: %s", meta_exc)
                    log_to_db("WARNING", "conversion", f"metadata.abs write failed: {meta_exc}", download_id=download_id)

                # Trigger ABS library scan + Quick Match (if configured)
                abs_url = get_setting(db, "abs_url")
                abs_token = get_setting(db, "abs_token")
                abs_library_id = get_setting(db, "abs_library_id")
                await _abs_scan_and_match(download_id, title, author, abs_url, abs_token, abs_library_id)
            else:
                dl.m4b_status = "m4b_failed"
                if _no_files:
                    reason = "no audio files found in the input path — check subdirectory structure"
                else:
                    reason = f"exit code {returncode}"
                logger.error("m4b-tool failed for download #%d: %s", download_id, reason)
                log_to_db("ERROR", "conversion", f"m4b-tool failed: {reason} — see DEBUG entry for full output", download_id=download_id)
            dl.conversion_log = log[-4000:]  # keep last 4000 chars
            db.commit()
    except Exception as exc:
        logger.error("M4B conversion error for download %d: %s", download_id, exc)
        try:
            dl = db.query(Download).filter(Download.id == download_id).first()
            if dl:
                dl.m4b_status = "m4b_failed"
                dl.conversion_log = str(exc)
                db.commit()
        except Exception:
            pass
    finally:
        db.close()


@router.post("/send", response_class=HTMLResponse)
async def send_to_nzbget(
    request: Request,
    nzb_hash: str = Form(...),
    nzb_title: str = Form(...),
    nzb_source: str = Form(default="nzbking"),
    search_term: str = Form(default=""),
    password: str = Form(default=""),
    post_title: str = Form(default=""),
    topic_id: str = Form(default=""),
    msg_id: str = Form(default=""),
    book_title: str = Form(default=""),
    book_author: str = Form(default=""),
    book_series: str = Form(default=""),
    book_series_part: str = Form(default=""),
    db: Session = Depends(get_db),
):
    # Retrieve NZBGet settings
    nzbget_url = get_setting(db, "nzbget_url")
    nzbget_username = get_setting(db, "nzbget_username")
    nzbget_password = get_setting(db, "nzbget_password")
    nzbget_category = get_setting(db, "nzbget_category")

    if not nzbget_url:
        return HTMLResponse(
            '<div class="alert alert-warning">'
            '<i class="bi bi-gear-fill me-2"></i>'
            'NZBGet is not configured. '
            '<a href="/settings" class="alert-link">Go to Settings</a></div>'
        )

    # Step 1: Download the NZB file from the appropriate source
    try:
        if nzb_source == "binsearch":
            nzb_content = BinsearchScraper().download_nzb(nzb_hash, name=nzb_title)
        else:
            nzb_content = NzbkingScraper().download_nzb(nzb_hash)
    except Exception as exc:
        logger.error("Failed to download NZB %s: %s", nzb_hash, exc)
        return HTMLResponse(
            f'<div class="alert alert-danger">'
            f'<i class="bi bi-exclamation-triangle-fill me-2"></i>'
            f"Failed to download NZB: {exc}</div>"
        )

    # Step 2: Send to NZBGet
    safe_name = _sanitize_filename(post_title or nzb_title or search_term or nzb_hash)
    try:
        client = NzbgetClient(nzbget_url, nzbget_username, nzbget_password)
        job_id = client.add_nzb(
            nzb_content,
            name=safe_name,
            category=nzbget_category,
            password=password,
        )
    except Exception as exc:
        logger.error("Failed to send NZB '%s' to NZBGet: %s", safe_name, exc)
        # Save a failed record
        record = Download(
            post_title=post_title,
            topic_id=topic_id,
            msg_id=msg_id,
            search_term=search_term,
            password=password,
            nzb_name=safe_name,
            nzb_hash=nzb_hash,
            nzbget_id=None,
            status="failed",
        )
        db.add(record)
        db.commit()
        return HTMLResponse(
            f'<div class="alert alert-danger">'
            f'<i class="bi bi-exclamation-triangle-fill me-2"></i>'
            f"Failed to send to NZBGet: {exc}</div>"
        )

    # Step 3: Save to DB
    # Pre-populate download_metadata from forum post if any fields were extracted
    initial_metadata = {}
    if book_title:       initial_metadata["title"]       = book_title
    if book_author:      initial_metadata["author"]      = book_author
    if book_series:      initial_metadata["series"]      = book_series
    if book_series_part: initial_metadata["series_part"] = book_series_part

    record = Download(
        post_title=post_title,
        topic_id=topic_id,
        msg_id=msg_id,
        search_term=search_term,
        password=password,
        nzb_name=safe_name,
        nzb_hash=nzb_hash,
        nzbget_id=job_id,
        status="sent",
        download_metadata=json.dumps(initial_metadata, ensure_ascii=False) if initial_metadata else None,
    )
    db.add(record)
    db.commit()

    logger.info("NZB '%s' sent to NZBGet with job ID %d", safe_name, job_id)
    return HTMLResponse(
        f'<div class="alert alert-success">'
        f'<i class="bi bi-check-circle-fill me-2"></i>'
        f"<strong>{safe_name}</strong> sent to NZBGet! "
        f"Job ID: <code>{job_id}</code> &mdash; "
        f'<a href="/downloads" class="alert-link">View Downloads</a></div>'
    )


@router.get("", response_class=HTMLResponse)
async def downloads_page(request: Request, db: Session = Depends(get_db)):
    downloads = db.query(Download).order_by(Download.created_at.desc()).all()
    return templates.TemplateResponse(
        "downloads.html",
        {"request": request, "downloads": downloads},
    )


def _apply_dl_filter(q, dl_filter: str):
    """Apply status filter to a Download query."""
    if dl_filter == "active":
        # Show: downloading (sent) OR downloaded-but-not-yet-converted
        # NULL m4b_status must be handled explicitly — SQL NULL != 'converted' evaluates to NULL not TRUE
        q = q.filter(
            or_(
                Download.status == "sent",
                and_(
                    Download.status == "downloaded",
                    or_(Download.m4b_status.is_(None), Download.m4b_status != "converted"),
                ),
            )
        )
    return q


@router.post("/sync-status", response_class=HTMLResponse)
async def sync_status(
    request: Request,
    background_tasks: BackgroundTasks,
    dl_filter: str = Form(default="active"),
    dl_limit: int = Form(default=5),
    db: Session = Depends(get_db),
):
    """
    Check nzbget history for any downloads still in 'sent' status and update them.
    Called by HTMX polling on the downloads page.
    """
    nzbget_url = get_setting(db, "nzbget_url")
    nzbget_username = get_setting(db, "nzbget_username")
    nzbget_password = get_setting(db, "nzbget_password")
    nzbget_prefix = get_setting(db, "nzbget_path_prefix")
    local_prefix = get_setting(db, "local_path_prefix")

    pending = db.query(Download).filter(
        Download.status == "sent", Download.nzbget_id.isnot(None)
    ).all()

    def _fetch_downloads():
        q = db.query(Download).order_by(Download.created_at.desc())
        q = _apply_dl_filter(q, dl_filter)
        if dl_limit > 0:
            q = q.limit(dl_limit)
        return q.all()

    if not pending or not nzbget_url:
        downloads = _fetch_downloads()
        return templates.TemplateResponse(
            "partials/download_rows.html",
            {"request": request, "downloads": downloads},
        )

    try:
        client = NzbgetClient(nzbget_url, nzbget_username, nzbget_password)
        history = client.get_history()
        history_by_id = {item.get("NZBID"): item for item in history}

        # Also fetch the active queue to get download progress %
        queue = client.get_queue()
        queue_by_id = {item.get("NZBID"): item for item in queue}

        newly_completed = []
        for dl in pending:
            # Update download progress from active queue
            q_item = queue_by_id.get(dl.nzbget_id)
            if q_item:
                file_mb = q_item.get("FileSizeMB", 0)
                remaining_mb = q_item.get("RemainingSizeMB", 0)
                if file_mb > 0:
                    pct = round((file_mb - remaining_mb) / file_mb * 100)
                    dl.download_progress = max(0, min(99, pct))

            # Check history for completion
            item = history_by_id.get(dl.nzbget_id)
            if item:
                raw_status = item.get("Status", "").upper()
                if "SUCCESS" in raw_status:
                    dl.status = "downloaded"
                    dl.download_progress = 100
                    # Capture the path where nzbget stored the files
                    final_dir = item.get("FinalDir") or item.get("DestDir", "")
                    if final_dir:
                        dl.download_path = _map_path(final_dir, nzbget_prefix, local_prefix)
                    newly_completed.append(dl.id)
                elif "FAILURE" in raw_status or "DELETED" in raw_status:
                    dl.status = "failed"
        db.commit()
        for dl_id in newly_completed:
            background_tasks.add_task(_auto_process_download, dl_id)
    except Exception as exc:
        logger.warning("sync_status: could not reach nzbget: %s", exc)

    downloads = _fetch_downloads()
    return templates.TemplateResponse(
        "partials/download_rows.html",
        {"request": request, "downloads": downloads},
    )


@router.get("/{download_id}/metadata-lookup", response_class=HTMLResponse)
async def metadata_lookup(
    request: Request,
    download_id: int,
    db: Session = Depends(get_db),
):
    """Search Audible via ABS and return the convert form pre-filled with results."""
    dl = db.query(Download).filter(Download.id == download_id).first()
    if not dl:
        return HTMLResponse('<div class="alert alert-danger">Download not found.</div>')

    abs_url = get_setting(db, "abs_url")
    abs_token = get_setting(db, "abs_token")

    # Existing saved metadata (paths we want to keep)
    saved = {}
    if dl.download_metadata:
        try:
            saved = json.loads(dl.download_metadata)
        except Exception:
            pass

    lookup_error = None
    if abs_url and abs_token:
        nzb_name     = dl.post_title or dl.nzb_name or ""
        nzb_title    = _extract_nzb_title(nzb_name)
        query_title  = nzb_title or saved.get("title") or ""
        query_author = saved.get("author") or _extract_nzb_author(nzb_name)
        try:
            log_to_db("INFO", "metadata",
                f"Manual Audible fetch #{download_id}: searching title='{query_title}' author='{query_author}'",
                download_id=download_id)
            client  = AbsClient(abs_url, abs_token)
            results = client.search_books(query_title.strip(), author=query_author.strip())
            if results:
                candidate_titles = [r.get("title", "") for r in results[:5]]
                log_to_db("DEBUG", "metadata",
                    f"Manual Audible fetch #{download_id}: candidates {candidate_titles}",
                    download_id=download_id)
                book           = _best_audible_match(results, nzb_title)
                audible_title  = book.get("title", "")
                audible_author = book.get("author") or book.get("authors") or ""
                audible_series, audible_series_part = _extract_audible_series(book)
                confidence     = _compute_confidence(nzb_title, query_author, audible_title, audible_author)

                log_to_db("DEBUG", "metadata", f"Audible raw result: {json.dumps(book, ensure_ascii=False)}", download_id=download_id)
                log_to_db("INFO", "metadata",
                    f"Manual Audible fetch #{download_id}: selected '{audible_title}' by '{audible_author}' — confidence {confidence}%",
                    download_id=download_id)

                # Explicit fetch — always replace with Audible data; fall back to saved if Audible has nothing
                saved["title"]            = audible_title       or saved.get("title", "")
                saved["author"]           = audible_author      or saved.get("author", "")
                saved["series"]           = audible_series      or saved.get("series", "")
                saved["series_part"]      = audible_series_part or saved.get("series_part", "")
                saved["match_confidence"] = confidence

                # Persist so the badge shows on the downloads page
                dl.download_metadata = json.dumps(saved, ensure_ascii=False)
                db.commit()
            else:
                lookup_error = f"No Audible results found for: {query_title}"
        except Exception as exc:
            logger.warning("metadata_lookup failed for download %d: %s", download_id, exc)
            lookup_error = f"Lookup failed: {exc}"
    else:
        lookup_error = "Audiobookshelf is not configured — cannot query Audible."

    m4b_output_path = get_setting(db, "m4b_output_path")
    safe_title = _sanitize_filename(saved.get("title") or dl.post_title or dl.nzb_name or "output")
    default_output = os.path.join(m4b_output_path, f"{safe_title}.m4b") if m4b_output_path else f"{safe_title}.m4b"

    return templates.TemplateResponse(
        "partials/convert_form.html",
        {
            "request": request,
            "dl": dl,
            "saved": saved,
            "suggested_output": saved.get("output_file") or default_output,
            "lookup_error": lookup_error,
        },
    )


@router.get("/{download_id}/convert", response_class=HTMLResponse)
async def get_convert_form(
    request: Request,
    download_id: int,
    db: Session = Depends(get_db),
):
    dl = db.query(Download).filter(Download.id == download_id).first()
    if not dl:
        return HTMLResponse('<div class="alert alert-danger">Download not found.</div>')
    if dl.status != "downloaded":
        return HTMLResponse('<div class="alert alert-warning">Download must be completed before converting.</div>')

    # Pre-fill from previously saved metadata if available
    saved = {}
    if dl.download_metadata:
        try:
            saved = json.loads(dl.download_metadata)
        except Exception:
            pass

    m4b_output_path = get_setting(db, "m4b_output_path")
    safe_title = _sanitize_filename(dl.post_title or dl.nzb_name or "output")
    default_output = os.path.join(m4b_output_path, f"{safe_title}.m4b") if m4b_output_path else f"{safe_title}.m4b"

    return templates.TemplateResponse(
        "partials/convert_form.html",
        {
            "request": request,
            "dl": dl,
            "saved": saved,
            "suggested_output": saved.get("output_file") or default_output,
        },
    )


@router.post("/{download_id}/save-metadata", response_class=HTMLResponse)
async def save_metadata(
    request: Request,
    download_id: int,
    title: str = Form(default=""),
    author: str = Form(default=""),
    series: str = Form(default=""),
    series_part: str = Form(default=""),
    input_path: str = Form(default=""),
    output_file: str = Form(default=""),
    db: Session = Depends(get_db),
):
    dl = db.query(Download).filter(Download.id == download_id).first()
    if not dl:
        return HTMLResponse('<div class="alert alert-danger">Download not found.</div>')

    saved = dl.parsed_metadata
    saved.update({
        "title":       title,
        "author":      author,
        "series":      series,
        "series_part": series_part,
        "input_path":  input_path,
        "output_file": output_file,
    })
    # Clear match_confidence so the next Fetch from Audible uses the new values
    saved.pop("match_confidence", None)
    dl.download_metadata = json.dumps(saved, ensure_ascii=False)
    db.commit()

    m4b_output_path = get_setting(db, "m4b_output_path")
    safe_title = _sanitize_filename(title or dl.post_title or dl.nzb_name or "output")
    default_output = os.path.join(m4b_output_path, f"{safe_title}.m4b") if m4b_output_path else f"{safe_title}.m4b"

    return templates.TemplateResponse(
        "partials/convert_form.html",
        {
            "request":        request,
            "dl":             dl,
            "saved":          saved,
            "suggested_output": output_file or default_output,
            "save_success":   True,
        },
    )


@router.post("/{download_id}/convert", response_class=HTMLResponse)
async def start_convert(
    request: Request,
    download_id: int,
    background_tasks: BackgroundTasks,
    title: str = Form(default=""),
    author: str = Form(default=""),
    series: str = Form(default=""),
    series_part: str = Form(default=""),
    input_path: str = Form(default=""),
    output_file: str = Form(default=""),
    db: Session = Depends(get_db),
):
    dl = db.query(Download).filter(Download.id == download_id).first()
    if not dl:
        return HTMLResponse('<div class="alert alert-danger">Download not found.</div>')
    if not input_path:
        return HTMLResponse('<div class="alert alert-warning">Input path is required.</div>')
    if not output_file:
        return HTMLResponse('<div class="alert alert-warning">Output file path is required.</div>')

    # Persist the conversion metadata so retries don't need re-entry
    dl.download_metadata = json.dumps({
        "title": title,
        "author": author,
        "series": series,
        "series_part": series_part,
        "input_path": input_path,
        "output_file": output_file,
    }, ensure_ascii=False)
    dl.m4b_status = "converting"
    dl.m4b_progress = None
    db.commit()

    background_tasks.add_task(
        _run_m4b_conversion,
        download_id, input_path, output_file, title, author, series, series_part,
    )

    display_name = title or dl.post_title or dl.nzb_name or f"Download #{download_id}"
    content = (
        f'<div class="alert alert-info">'
        f'<i class="bi bi-arrow-repeat me-2"></i>'
        f'<strong>{display_name}</strong> is being converted to M4B. '
        f'Status will update automatically on this page.</div>'
    )
    response = HTMLResponse(content)
    response.headers["HX-Trigger"] = "refreshDownloads"
    return response


@router.delete("/{download_id}", response_class=HTMLResponse)
async def delete_download(download_id: int, db: Session = Depends(get_db)):
    dl = db.query(Download).filter(Download.id == download_id).first()
    if dl:
        db.delete(dl)
        db.commit()
    return HTMLResponse("")  # HTMX swaps the row with nothing, removing it


@router.get("/{download_id}/conversion-log", response_class=HTMLResponse)
async def conversion_log(
    request: Request,
    download_id: int,
    db: Session = Depends(get_db),
):
    dl = db.query(Download).filter(Download.id == download_id).first()
    if not dl or not dl.conversion_log:
        return HTMLResponse('<p class="text-muted small mb-0">No log available.</p>')
    escaped = dl.conversion_log.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")
    return HTMLResponse(
        f'<pre class="bc-raw-text mb-0" style="max-height:300px;overflow-y:auto;font-size:0.75rem;">{escaped}</pre>'
    )
