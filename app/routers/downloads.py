import asyncio
import json
import logging
import os
import re
import shutil
import unicodedata
from fastapi import APIRouter, BackgroundTasks, Depends, Form, Request
from fastapi.responses import HTMLResponse, Response
from fastapi.templating import Jinja2Templates
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


@router.post("/sync-status", response_class=HTMLResponse)
async def sync_status(request: Request, db: Session = Depends(get_db)):
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

    if not pending or not nzbget_url:
        # Nothing to check — return current table rows silently
        downloads = db.query(Download).order_by(Download.created_at.desc()).all()
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
                elif "FAILURE" in raw_status or "DELETED" in raw_status:
                    dl.status = "failed"
        db.commit()
    except Exception as exc:
        logger.warning("sync_status: could not reach nzbget: %s", exc)

    downloads = db.query(Download).order_by(Download.created_at.desc()).all()
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
        query = dl.search_term or dl.post_title or dl.nzb_name or ""
        try:
            client = AbsClient(abs_url, abs_token)
            results = client.search_books(query.strip())
            if results:
                book = results[0]
                # ABS returns author as a string field (varies by provider)
                author_raw = book.get("author") or book.get("authors") or ""
                saved["title"] = book.get("title") or saved.get("title", "")
                saved["author"] = author_raw
                # Series may be a plain string or an array [{series, volumeNumber}]
                series_raw = book.get("series")
                if isinstance(series_raw, list) and series_raw:
                    saved["series"] = series_raw[0].get("series") or series_raw[0].get("name", "")
                    saved["series_part"] = str(series_raw[0].get("volumeNumber", ""))
                elif isinstance(series_raw, str):
                    saved["series"] = series_raw
                    saved["series_part"] = str(book.get("volumeNumber") or saved.get("series_part", ""))
                log_to_db("INFO", "metadata", f"Audible lookup for #{download_id}: {saved.get('title')} by {saved.get('author')}", download_id=download_id)
            else:
                lookup_error = f"No Audible results found for: {query}"
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
