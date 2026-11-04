"""
The Wishlist page: books, authors and series BookClub watches the forum for, and
the series in your Audiobookshelf library with their missing books.
"""
import time

from fastapi import APIRouter, Depends, Form, Request
from fastapi.responses import HTMLResponse
from sqlalchemy.orm import Session

from app.database import get_db
from app.models import BookRequest, WishlistItem
from app.services import book_requests, health, wishlist
from app.services.pipeline import abs_configured, pipeline
from app.settings_store import get_setting
from app.templating import templates
from app.ui import alert, alert_html

router = APIRouter()

SERIES_CACHE_SECONDS = 600
_series_cache: dict = {"at": 0.0, "key": "", "series": []}


def _rows(request: Request, db: Session, notice: str | None = ""):
    items = db.query(WishlistItem).order_by(WishlistItem.id.desc()).all()
    request_ids = [i.request_id for i in items if i.request_id]
    req_status = {r.id: r.status for r in db.query(BookRequest).filter(BookRequest.id.in_(request_ids)).all()} \
        if request_ids else {}
    return templates.TemplateResponse("partials/wishlist_rows.html", {
        "request": request, "items": items, "notice": notice, "kinds": wishlist.KINDS, "req_status": req_status,
        "check_hours": get_setting(db, "wishlist_check_hours"),
    })


@router.get("", response_class=HTMLResponse)
def wishlist_page(request: Request, db: Session = Depends(get_db)):
    return templates.TemplateResponse("wishlist.html", {
        "request": request, "kinds": wishlist.KINDS, "abs_ready": abs_configured(db),
        "check_hours": get_setting(db, "wishlist_check_hours"),
    })


@router.get("/rows", response_class=HTMLResponse)
def wishlist_rows(request: Request, db: Session = Depends(get_db)):
    return _rows(request, db, notice=None)


@router.post("", response_class=HTMLResponse)
def add(request: Request, query: str = Form(default=""), kind: str = Form(default="book"),
        author: str = Form(default=""), auto_download: str = Form(default=""), db: Session = Depends(get_db)):
    item, error = wishlist.add_item(db, kind, query, author, auto_download == "1")
    if error:
        return _rows(request, db, alert_html("warning", error))
    pipeline.run_soon(pipeline.check_wishlist_item, item.id)
    what = {"book": "this book", "author": "new books by this author", "series": "this series"}[item.kind]
    return _rows(request, db, alert_html("success", f"Watching the forum for {what}: {item.query}. "
                                                   "Checking now…"))


@router.post("/{item_id}/check", response_class=HTMLResponse)
def check(request: Request, item_id: int, db: Session = Depends(get_db)):
    item = db.get(WishlistItem, item_id)
    if item:
        if item.status == "done":
            item.status = "watching"
        item.message = "Checking the forum…"
        db.commit()
        pipeline.run_soon(pipeline.check_wishlist_item, item.id)
    return _rows(request, db)


@router.post("/{item_id}/pause", response_class=HTMLResponse)
def pause(request: Request, item_id: int, db: Session = Depends(get_db)):
    item = db.get(WishlistItem, item_id)
    if item:
        item.status = "watching" if item.status == "paused" else "paused"
        item.next_check_at = None if item.status == "watching" else item.next_check_at
        item.message = "Paused — not checking the forum" if item.status == "paused" else "Watching again"
        db.commit()
    return _rows(request, db)


@router.post("/{item_id}/auto", response_class=HTMLResponse)
def toggle_auto(request: Request, item_id: int, db: Session = Depends(get_db)):
    item = db.get(WishlistItem, item_id)
    if item:
        item.auto_download = not item.auto_download
        db.commit()
    return _rows(request, db)


@router.post("/{item_id}/get", response_class=HTMLResponse)
def get_found(request: Request, item_id: int, topic_id: str = Form(...), db: Session = Depends(get_db)):
    item = db.get(WishlistItem, item_id)
    req = wishlist.take_found(db, item, topic_id) if item else None
    if not req:
        return _rows(request, db, alert_html("warning", "That book is no longer in the list."))
    pipeline.run_soon(pipeline.process_request, req.id)
    return _rows(request, db, alert_html("success", f"Requested '{req.query}' — follow it under Requests."))


@router.delete("/{item_id}", response_class=HTMLResponse)
def remove(request: Request, item_id: int, db: Session = Depends(get_db)):
    item = db.get(WishlistItem, item_id)
    if item:
        db.delete(item)
        db.commit()
    return _rows(request, db)


# ── series in the library ─────────────────────────────────────────────────────

@router.get("/series", response_class=HTMLResponse)
def series_list(request: Request, refresh: str = "", db: Session = Depends(get_db)):
    if not abs_configured(db):
        return alert("info", "Connect Audiobookshelf in Settings to see the series in your library.")
    key = f"{get_setting(db, 'abs_url')}|{get_setting(db, 'abs_library_id')}"
    fresh = _series_cache["key"] == key and time.monotonic() - _series_cache["at"] < SERIES_CACHE_SECONDS
    if refresh or not fresh:
        try:
            _series_cache.update(series=health.library_series(db), at=time.monotonic(), key=key)
        except Exception as exc:
            return alert("danger", f"Could not read the series from Audiobookshelf: {exc}")
    followed = {wishlist.normalize(i.query) for i in db.query(WishlistItem).filter(WishlistItem.kind == "series").all()}
    rows = [dict(series, followed=wishlist.normalize(series["name"]) in followed) for series in _series_cache["series"]]
    return templates.TemplateResponse("partials/series_rows.html", {"request": request, "series": rows})


@router.get("/series/forum", response_class=HTMLResponse)
def series_forum(request: Request, name: str, author: str = "", refresh: str = "", db: Session = Depends(get_db)):
    """The series' books on the forum, marked with the ones you have (plain `def`: asks the forum)."""
    scraper = book_requests._scraper(db)
    if scraper is None:
        return alert("warning", "abook.link is not set up in Settings.")
    try:
        topics = wishlist.missing_from_series(db, scraper, name, "", refresh=bool(refresh))
    except Exception as exc:
        return alert("danger", f"Forum search failed: {exc}")
    return templates.TemplateResponse("partials/series_forum.html", {
        "request": request, "name": name, "author": author, "topics": topics,
        "missing": [t for t in topics if not t["owned"]],
    })


@router.post("/request-topic", response_class=HTMLResponse)
def request_topic(topic_id: str = Form(...), title: str = Form(...), db: Session = Depends(get_db)):
    req = wishlist.request_topic(db, topic_id, title, "")
    pipeline.run_soon(pipeline.process_request, req.id)
    return HTMLResponse('<span class="badge bc-badge-success"><i class="bi bi-check-circle-fill me-1"></i>Requested</span>')
