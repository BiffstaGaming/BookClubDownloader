import logging

from fastapi import APIRouter, Depends, Form, Request
from fastapi.responses import HTMLResponse
from sqlalchemy.orm import Session

from app.database import get_db
from app.scrapers.abook import get_scraper
from app.services.nzb import search_nzbs
from app.services.pipeline import find_duplicates
from app.settings_store import get_setting
from app.templating import templates
from app.ui import alert, not_configured

logger = logging.getLogger(__name__)
router = APIRouter()

# Route handlers are plain `def` so FastAPI runs them in its thread pool —
# the scrapers block on network I/O and must not stall the event loop.


def _get_abook_scraper(db: Session):
    """Shared logged-in scraper for the saved credentials, or None if not configured."""
    base_url = get_setting(db, "abook_url")
    username = get_setting(db, "abook_username")
    password = get_setting(db, "abook_password")
    if not base_url or not username or not password:
        return None
    return get_scraper(base_url, username, password)


def _topic_url(db: Session, topic_id: str) -> str:
    abook_url = get_setting(db, "abook_url") or ""
    return f"{abook_url.rstrip('/')}?topic={topic_id}" if abook_url else ""


@router.post("/forum", response_class=HTMLResponse)
def search_forum(
    request: Request,
    query: str = Form(...),
    db: Session = Depends(get_db),
):
    scraper = _get_abook_scraper(db)
    if scraper is None:
        return not_configured("abook.link")
    try:
        with scraper.lock:
            results = scraper.search(query)
    except Exception as exc:
        logger.error("Forum search error: %s", exc)
        return alert("danger", f"Forum search failed: {exc}")

    return templates.TemplateResponse(
        "partials/forum_results.html",
        {"request": request, "results": results, "query": query},
    )


@router.get("/topic/{topic_id}", response_class=HTMLResponse)
def get_topic(
    request: Request,
    topic_id: str,
    db: Session = Depends(get_db),
):
    scraper = _get_abook_scraper(db)
    if scraper is None:
        return not_configured("abook.link")
    try:
        with scraper.lock:
            topic = scraper.get_topic(topic_id)
    except Exception as exc:
        logger.error("Get topic error for topic %s: %s", topic_id, exc)
        return alert("danger", f"Failed to load topic: {exc}")

    return templates.TemplateResponse(
        "partials/topic_content.html",
        {
            "request": request,
            "topic": topic,
            "topic_url": _topic_url(db, topic_id),
            "revealed_posts": {},
            "auto_search": False,
        },
    )


@router.post("/thank", response_class=HTMLResponse)
def thank_post(
    request: Request,
    topic_id: str = Form(...),
    msg_id: str = Form(...),
    thank_href: str = Form(...),
    post_title: str = Form(default=""),
    db: Session = Depends(get_db),
):
    scraper = _get_abook_scraper(db)
    if scraper is None:
        return not_configured("abook.link")

    try:
        with scraper.lock:
            revealed = scraper.thank_and_get_content(topic_id, msg_id, thank_href)
    except Exception as exc:
        logger.error("Thank post error for msg %s: %s", msg_id, exc)
        return alert("danger", f"Failed to thank post: {exc}")

    # Re-fetch topic to get full post list
    try:
        with scraper.lock:
            topic = scraper.get_topic(topic_id)
    except Exception as exc:
        logger.error("Re-fetch topic error: %s", exc)
        topic = {"topic_id": topic_id, "title": post_title, "posts": []}

    revealed_posts = {
        msg_id: {
            "search_term": revealed.get("search_term", ""),
            "password":    revealed.get("password", ""),
            "raw_text":    revealed.get("raw_text", ""),
            "title":       revealed.get("title", ""),
            "author":      revealed.get("author", ""),
            "series":      revealed.get("series", ""),
            "series_part": revealed.get("series_part", ""),
            "narrator":    revealed.get("narrator", ""),
        }
    }

    return templates.TemplateResponse(
        "partials/topic_content.html",
        {
            "request": request,
            "topic": topic,
            "topic_url": _topic_url(db, topic_id),
            "post_title": post_title,
            "revealed_posts": revealed_posts,
            # Freshly revealed — kick off the NZB search straight away
            "auto_search": bool(revealed.get("search_term")),
        },
    )


@router.post("/nzb", response_class=HTMLResponse)
def search_nzb(
    request: Request,
    search_term: str = Form(...),
    nzb_source: str = Form(default="nzbking"),
    password: str = Form(default=""),
    post_title: str = Form(default=""),
    topic_id: str = Form(default=""),
    msg_id: str = Form(default=""),
    book_title: str = Form(default=""),
    book_author: str = Form(default=""),
    book_series: str = Form(default=""),
    book_series_part: str = Form(default=""),
    book_narrator: str = Form(default=""),
    replace_id: str = Form(default=""),
    db: Session = Depends(get_db),
):
    term = search_term.strip()
    if not term:
        return alert("warning", "No search term provided.")

    # Try the chosen indexer first, then the other one if it has nothing.
    sources = [nzb_source, "binsearch" if nzb_source != "binsearch" else "nzbking"]
    results, notes, errors = [], [], []
    for source in sources:
        label = "Binsearch" if source == "binsearch" else "NZBKing"
        try:
            results = search_nzbs(source, term)
        except Exception as exc:
            logger.error("%s search error for '%s': %s", label, term, exc)
            errors.append(f"{label} search failed: {exc}")
            continue
        if results:
            if source != nzb_source:
                notes.append(f"Nothing on {'Binsearch' if nzb_source == 'binsearch' else 'NZBKing'} — showing {label} results.")
            break
    if not results and errors and len(errors) == len(sources):
        return alert("danger", " · ".join(errors))

    duplicates = []
    if not replace_id:
        try:
            duplicates = find_duplicates(db, book_title or post_title, book_author, search_term=term)
        except Exception as exc:
            logger.warning("Duplicate check failed: %s", exc)

    return templates.TemplateResponse(
        "partials/nzb_results.html",
        {
            "request": request,
            "results": results,
            "notes": notes + errors,
            "duplicates": duplicates,
            "search_term": search_term,
            "password": password,
            "post_title": post_title,
            "topic_id": topic_id,
            "msg_id": msg_id,
            "book_title": book_title,
            "book_author": book_author,
            "book_series": book_series,
            "book_series_part": book_series_part,
            "book_narrator": book_narrator,
            "replace_id": replace_id,
        },
    )
