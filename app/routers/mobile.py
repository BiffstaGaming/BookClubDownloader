"""
The phone UI (/m): the same features as the desktop pages, laid out for a phone —
cards instead of tables, big buttons, a bottom tab bar. Every htmx request from
these pages carries `X-BC-UI: mobile`, and the shared routes then answer with the
phone version of their fragments (see app/templating.py), so all the behaviour
stays in one place.

Phones that open a desktop page are sent here; "Desktop view" (More tab) sets a
cookie that stops that, and "Phone view" on the desktop pages clears it again.
"""
from fastapi import APIRouter, Depends, Request
from fastapi.responses import HTMLResponse, RedirectResponse
from sqlalchemy.orm import Session

from app.database import get_db
from app.models import Download, RevealedPost
from app.routers.api import status as api_status
from app.services import ai
from app.settings_store import get_setting
from app.templating import templates

router = APIRouter()

UI_COOKIE = "bc_ui"
COOKIE_AGE = 60 * 60 * 24 * 365


def wants_phone_ui(request: Request) -> bool:
    """A phone browser that hasn't asked for the desktop pages."""
    agent = request.headers.get("user-agent", "")
    return "Mobi" in agent and request.cookies.get(UI_COOKIE) != "desktop"


def _page(request: Request, name: str, **context):
    return templates.TemplateResponse(f"mobile/{name}", {"request": request, **context})


@router.get("", response_class=HTMLResponse)
def home(request: Request):
    return _page(request, "home.html")


@router.get("/status", response_class=HTMLResponse)
def status_cards(request: Request, db: Session = Depends(get_db)):
    """What's happening now, for the Home tab (refreshed in the background)."""
    downloading = (db.query(Download).filter(Download.status == "sent")
                   .order_by(Download.created_at.desc()).limit(4).all())
    converting = db.query(Download).filter(Download.m4b_status == "converting").first()
    recent = (db.query(Download).filter(Download.imported_at.isnot(None))
              .order_by(Download.imported_at.desc()).limit(5).all())
    return templates.TemplateResponse("mobile/partials/status.html", {
        "request": request, "s": api_status(db), "downloading": downloading, "converting": converting,
        "recent": recent, "abs_url": get_setting(db, "abs_url").rstrip("/"),
    })


@router.get("/search", response_class=HTMLResponse)
def search(request: Request):
    return _page(request, "search.html")


@router.get("/downloads", response_class=HTMLResponse)
def downloads(request: Request):
    return _page(request, "downloads.html")


@router.get("/saved", response_class=HTMLResponse)
def saved(request: Request, db: Session = Depends(get_db)):
    return _page(request, "saved.html", total=db.query(RevealedPost).count())


@router.get("/more", response_class=HTMLResponse)
def more(request: Request, db: Session = Depends(get_db)):
    return _page(request, "more.html", ai_on=get_setting(db, "ai_enabled") == "1",
                 ai_usage=ai.usage(db), ha_on=bool(get_setting(db, "ha_webhook_url")))


@router.get("/desktop")
def use_desktop(next: str = "/"):
    """Stop sending this phone to /m."""
    response = RedirectResponse(next if next.startswith("/") and not next.startswith("//") else "/", status_code=303)
    response.set_cookie(UI_COOKIE, "desktop", max_age=COOKIE_AGE, samesite="lax")
    return response


@router.get("/phone")
def use_phone():
    """Back to the phone UI (and to automatic redirects)."""
    response = RedirectResponse("/m", status_code=303)
    response.delete_cookie(UI_COOKIE)
    return response


# Desktop page → phone page, for phones (query string kept: /downloads?highlight=5 from a notification).
PHONE_PAGES = {"/": "/m", "/downloads": "/m/downloads", "/saved": "/m/saved"}
