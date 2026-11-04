"""The Library page: a health check of the Audiobookshelf library, with fixes you approve."""
from fastapi import APIRouter, Depends, Form, Request
from fastapi.responses import HTMLResponse
from sqlalchemy.orm import Session

from app.database import get_db
from app.services import ai, health
from app.services.pipeline import abs_configured
from app.settings_store import get_setting
from app.templating import templates
from app.ui import alert_html

router = APIRouter()


def _results(request: Request, db: Session, notice: str = ""):
    data = health.load(db)
    counts = {}
    for issue in data["issues"]:
        counts[issue["kind"]] = counts.get(issue["kind"], 0) + 1
    return templates.TemplateResponse("partials/health_rows.html", {
        "request": request, "data": data, "counts": counts, "labels": health.ISSUE_LABELS, "notice": notice,
        "abs_url": get_setting(db, "abs_url").rstrip("/"), "abs_ready": abs_configured(db),
        "ai_reason": ai.unavailable_reason(db), "suggest_kinds": health.SUGGEST_KINDS,
    })


@router.get("", response_class=HTMLResponse)
def library_page(request: Request):
    return templates.TemplateResponse("library.html", {"request": request})


@router.get("/health", response_class=HTMLResponse)
def health_results(request: Request, db: Session = Depends(get_db)):
    return _results(request, db)


@router.post("/health/scan", response_class=HTMLResponse)
def scan(request: Request, db: Session = Depends(get_db)):
    if not abs_configured(db):
        return _results(request, db, alert_html("warning", "Connect Audiobookshelf in Settings first."))
    health.start_scan()
    return _results(request, db)


@router.post("/health/suggest", response_class=HTMLResponse)
def suggest(request: Request, db: Session = Depends(get_db)):
    """Plain `def`: waits for Claude."""
    try:
        message = health.suggest(db)
        notice = alert_html("info", message)
    except Exception as exc:
        notice = alert_html("warning", f"Claude couldn't help: {exc}")
    return _results(request, db, notice)


@router.post("/health/apply", response_class=HTMLResponse)
def apply(request: Request, suggestion_id: str = Form(...), db: Session = Depends(get_db)):
    try:
        notice = alert_html("success", health.apply_suggestion(db, suggestion_id))
    except Exception as exc:
        notice = alert_html("danger", f"Not changed: {exc}")
    return _results(request, db, notice)


@router.post("/health/dismiss", response_class=HTMLResponse)
def dismiss(request: Request, suggestion_id: str = Form(...), db: Session = Depends(get_db)):
    health.dismiss_suggestion(db, suggestion_id)
    return _results(request, db)


@router.post("/health/match", response_class=HTMLResponse)
def quick_match(request: Request, item_id: str = Form(...), db: Session = Depends(get_db)):
    try:
        notice = alert_html("success", health.quick_match(db, item_id))
    except Exception as exc:
        notice = alert_html("danger", f"Quick Match failed: {exc}")
    return _results(request, db, notice)
