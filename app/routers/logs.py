import logging
from fastapi import APIRouter, Depends, Query, Request
from fastapi.responses import HTMLResponse
from sqlalchemy.orm import Session

from app.database import get_db
from app.models import LogEntry
from app.templating import templates

logger = logging.getLogger(__name__)
router = APIRouter()

# Plain `def` handlers: FastAPI runs them in a thread, so slow log queries don't
# hold up the event loop (which also runs the background pipeline).

VALID_LEVELS = {"DEBUG", "INFO", "WARNING", "ERROR"}
DEFAULT_LOG_LIMIT = 50


@router.get("", response_class=HTMLResponse)
def logs_page(request: Request, db: Session = Depends(get_db)):
    return templates.TemplateResponse("logs.html", {"request": request})


@router.get("/entries", response_class=HTMLResponse)
def log_entries(
    request: Request,
    level: str = Query(default="ALL"),
    limit: int = Query(default=DEFAULT_LOG_LIMIT),
    download: str = Query(default=""),
    search: str = Query(default=""),
    db: Session = Depends(get_db),
):
    q = db.query(LogEntry)
    if level.upper() in VALID_LEVELS:
        q = q.filter(LogEntry.level == level.upper())
    if download.strip().lstrip("#").isdecimal():
        q = q.filter(LogEntry.download_id == int(download.strip().lstrip("#")))
    if search.strip():
        q = q.filter(LogEntry.message.ilike(f"%{search.strip()}%"))
    effective_limit = max(1, limit) if limit > 0 else 500
    entries = q.order_by(LogEntry.created_at.desc()).limit(effective_limit).all()
    return templates.TemplateResponse(
        "partials/log_rows.html",
        {"request": request, "entries": entries, "level": level},
    )


@router.delete("", response_class=HTMLResponse)
def clear_logs(request: Request, db: Session = Depends(get_db)):
    db.query(LogEntry).delete()
    db.commit()
    logger.info("Log history cleared by user")
    return templates.TemplateResponse(
        "partials/log_rows.html",
        {"request": request, "entries": [], "level": "ALL"},
    )
