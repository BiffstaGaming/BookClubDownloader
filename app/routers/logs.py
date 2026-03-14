import logging
from fastapi import APIRouter, Depends, Query, Request
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates
from sqlalchemy.orm import Session

from app.database import get_db
from app.models import LogEntry

logger = logging.getLogger(__name__)
router = APIRouter()
templates = Jinja2Templates(directory="app/templates")

VALID_LEVELS = {"DEBUG", "INFO", "WARNING", "ERROR"}
PAGE_SIZE = 500


@router.get("", response_class=HTMLResponse)
async def logs_page(request: Request, db: Session = Depends(get_db)):
    return templates.TemplateResponse("logs.html", {"request": request})


@router.get("/entries", response_class=HTMLResponse)
async def log_entries(
    request: Request,
    level: str = Query(default="ALL"),
    db: Session = Depends(get_db),
):
    q = db.query(LogEntry)
    if level.upper() in VALID_LEVELS:
        q = q.filter(LogEntry.level == level.upper())
    entries = q.order_by(LogEntry.created_at.desc()).limit(PAGE_SIZE).all()
    return templates.TemplateResponse(
        "partials/log_rows.html",
        {"request": request, "entries": entries, "level": level},
    )


@router.delete("", response_class=HTMLResponse)
async def clear_logs(request: Request, db: Session = Depends(get_db)):
    db.query(LogEntry).delete()
    db.commit()
    logger.info("Log history cleared by user")
    return templates.TemplateResponse(
        "partials/log_rows.html",
        {"request": request, "entries": [], "level": "ALL"},
    )
