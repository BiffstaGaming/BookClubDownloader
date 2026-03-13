import logging
import re
from fastapi import APIRouter, Depends, Form, Request
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates
from sqlalchemy.orm import Session

from app.database import get_db
from app.models import Download
from app.scrapers.nzbking import NzbkingScraper
from app.services.nzbget import NzbgetClient
from app.routers.settings import get_setting

logger = logging.getLogger(__name__)
router = APIRouter()
templates = Jinja2Templates(directory="app/templates")


def _sanitize_filename(name: str) -> str:
    """Remove characters that are invalid in filenames."""
    name = re.sub(r'[<>:"/\\|?*]', "_", name)
    name = name.strip(". ")
    return name or "download"


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
        # history items have 'NZBID' and 'Status' fields
        history_by_id = {item.get("NZBID"): item for item in history}

        for dl in pending:
            item = history_by_id.get(dl.nzbget_id)
            if item:
                raw_status = item.get("Status", "").upper()
                if "SUCCESS" in raw_status:
                    dl.status = "downloaded"
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
