"""
The Saved page: every forum post BookClub has revealed (search term, password,
book details), kept for good — so a book can be sent to NZBGet again after lost
data without searching or thanking on the forum.
"""
import logging

from fastapi import APIRouter, Depends, Request
from fastapi.responses import HTMLResponse
from markupsafe import escape
from sqlalchemy import or_
from sqlalchemy.orm import Session

from app.database import get_db
from app.models import Download, RevealedPost
from app.services import book_requests
from app.services.nzb import find_nzbs
from app.settings_store import get_setting
from app.templating import templates
from app.ui import alert, alert_html, not_configured

logger = logging.getLogger(__name__)
router = APIRouter()

# Plain `def` handlers: sending searches the indexers and talks to NZBGet.


def _downloads_by_term(db: Session, posts: list[RevealedPost]) -> dict[str, Download]:
    """The most recent download for each post's search term."""
    terms = [p.search_term for p in posts]
    latest = {}
    if terms:
        for dl in db.query(Download).filter(Download.search_term.in_(terms)).order_by(Download.id).all():
            latest[dl.search_term] = dl
    return latest


def _row(request: Request, db: Session, post: RevealedPost, notice: str = ""):
    return templates.TemplateResponse(
        "partials/saved_rows.html",
        {"request": request, "posts": [post], "downloads": _downloads_by_term(db, [post]),
         "notices": {post.id: notice} if notice else {}, "topic_base": _topic_base(db)},
    )


def _topic_base(db: Session) -> str:
    url = get_setting(db, "abook_url") or ""
    return f"{url.rstrip('/')}?topic=" if url else ""


@router.get("", response_class=HTMLResponse)
def saved_page(request: Request, db: Session = Depends(get_db)):
    return templates.TemplateResponse("saved.html", {"request": request, "total": db.query(RevealedPost).count()})


@router.get("/rows", response_class=HTMLResponse)
def saved_rows(request: Request, q: str = "", limit: int = 50, db: Session = Depends(get_db)):
    query = db.query(RevealedPost)
    for word in q.split()[:6]:
        like = f"%{word}%"
        query = query.filter(or_(RevealedPost.title.ilike(like), RevealedPost.author.ilike(like),
                                 RevealedPost.series.ilike(like), RevealedPost.topic_title.ilike(like),
                                 RevealedPost.narrator.ilike(like), RevealedPost.search_term.ilike(like)))
    query = query.order_by(RevealedPost.revealed_at.desc())
    posts = query.limit(limit).all() if limit > 0 else query.all()
    return templates.TemplateResponse(
        "partials/saved_rows.html",
        {"request": request, "posts": posts, "downloads": _downloads_by_term(db, posts), "notices": {},
         "topic_base": _topic_base(db)},
    )


@router.post("/{post_id}/send", response_class=HTMLResponse)
def send_again(request: Request, post_id: int, db: Session = Depends(get_db)):
    """Find the NZB for the saved search term and send it to NZBGet — no forum visit."""
    post = db.get(RevealedPost, post_id)
    if not post:
        return HTMLResponse("")
    if not get_setting(db, "nzbget_url"):
        return _row(request, db, post, alert_html("warning", "NZBGet is not configured — see Settings."))
    try:
        # force: re-downloading on purpose, so NZBGet must not skip it as a duplicate
        dl = book_requests.send_post(db, post.as_revealed(), post.topic_id or "", post.msg_id or "",
                                     post.topic_title or post.display_title, force=True)
    except book_requests.NoNzbFound as exc:
        return _row(request, db, post, alert_html(
            "warning", f"{exc}. Try 'Choose NZB', or open the post on the forum for a newer upload."))
    except Exception as exc:
        logger.error("Sending saved post %d failed: %s", post_id, exc)
        return _row(request, db, post, alert_html("danger", f"Could not send: {exc}"))
    db.refresh(post)
    link = f' <a href="/downloads?highlight={dl.id}" class="alert-link">Show the download</a>'
    return _row(request, db, post, alert_html("success", f"Sent to NZBGet (job {dl.nzbget_id}).", link))


@router.get("/{post_id}/nzbs", response_class=HTMLResponse)
def choose_nzb(request: Request, post_id: int, db: Session = Depends(get_db)):
    """All NZBs for the saved search term, to pick one by hand."""
    post = db.get(RevealedPost, post_id)
    if not post:
        return alert("danger", "Saved post not found.")
    if not get_setting(db, "nzbget_url"):
        return not_configured("NZBGet")
    results, notes = find_nzbs(post.search_term, every_source=True)
    return templates.TemplateResponse(
        "partials/nzb_results.html",
        {
            "request": request, "results": results, "notes": notes, "duplicates": [],
            "search_term": post.search_term, "password": post.password or "",
            "post_title": post.topic_title or post.display_title, "topic_id": post.topic_id or "",
            "msg_id": post.msg_id or "", "book_title": post.title or "", "book_author": post.author or "",
            "book_series": post.series or "", "book_series_part": post.series_part or "",
            "book_narrator": post.narrator or "", "replace_id": "",
        },
    )


@router.delete("/{post_id}", response_class=HTMLResponse)
def forget(post_id: int, db: Session = Depends(get_db)):
    post = db.get(RevealedPost, post_id)
    if post:
        logger.info("Saved post for '%s' removed", escape(post.display_title))
        db.delete(post)
        db.commit()
    # The row disappears; the count at the top is updated out-of-band.
    return HTMLResponse(f'<span id="saved-total" hx-swap-oob="true">{db.query(RevealedPost).count()}</span>')
