import asyncio
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
from app.services.nzbget import NzbgetClient
from app.routers.settings import get_setting
from app.log_handler import log_to_db

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


def _map_path(path: str, nzbget_prefix: str, local_prefix: str) -> str:
    """Replace the nzbget path prefix with the local path prefix."""
    if nzbget_prefix and local_prefix and path.startswith(nzbget_prefix):
        return local_prefix.rstrip("/") + path[len(nzbget_prefix):]
    return path


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
        cmd = [
            "m4b-tool", "merge", input_path,
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

        logger.info("Starting m4b-tool for download #%d: %s", download_id, " ".join(cmd))
        log_to_db("INFO", "conversion", f"Starting m4b-tool conversion", download_id=download_id)
        proc = await asyncio.create_subprocess_exec(
            *cmd,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.STDOUT,
        )
        stdout, _ = await proc.communicate()
        log = stdout.decode("utf-8", errors="replace")

        # Store the full m4b-tool output as a DEBUG entry linked to this download
        log_to_db("DEBUG", "conversion", f"m4b-tool output:\n{log}", download_id=download_id)

        dl = db.query(Download).filter(Download.id == download_id).first()
        if dl:
            if proc.returncode == 0:
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
            else:
                dl.m4b_status = "m4b_failed"
                logger.error("m4b-tool exited with code %d for download #%d — check DEBUG log for output", proc.returncode, download_id)
                log_to_db("ERROR", "conversion", f"m4b-tool failed (exit code {proc.returncode}) — see DEBUG entry for full output", download_id=download_id)
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

    # Step 1: Download the NZB file
    try:
        nzb_scraper = NzbkingScraper()
        nzb_content = nzb_scraper.download_nzb(nzb_hash)
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

        for dl in pending:
            item = history_by_id.get(dl.nzbget_id)
            if item:
                raw_status = item.get("Status", "").upper()
                if "SUCCESS" in raw_status:
                    dl.status = "downloaded"
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

    m4b_output_path = get_setting(db, "m4b_output_path")
    safe_title = _sanitize_filename(dl.post_title or dl.nzb_name or "output")

    # Suggest output filename
    suggested_output = os.path.join(m4b_output_path, f"{safe_title}.m4b") if m4b_output_path else f"{safe_title}.m4b"

    return templates.TemplateResponse(
        "partials/convert_form.html",
        {
            "request": request,
            "dl": dl,
            "suggested_output": suggested_output,
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

    dl.m4b_status = "converting"
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
