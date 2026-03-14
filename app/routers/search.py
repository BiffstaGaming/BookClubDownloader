import asyncio
import logging
from fastapi import APIRouter, Depends, Form, Request
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates
from sqlalchemy.orm import Session

from app.database import get_db
from app.scrapers.abook import AbookScraper
from app.scrapers.nzbking import NzbkingScraper
from app.scrapers.binsearch import BinsearchScraper
from app.routers.settings import get_setting

logger = logging.getLogger(__name__)
router = APIRouter()
templates = Jinja2Templates(directory="app/templates")


def _get_abook_scraper(db: Session):
    """Build an AbookScraper from DB settings, or return None if not configured."""
    base_url = get_setting(db, "abook_url")
    username = get_setting(db, "abook_username")
    password = get_setting(db, "abook_password")
    if not base_url or not username or not password:
        return None
    return AbookScraper(base_url, username, password)


@router.post("/forum", response_class=HTMLResponse)
async def search_forum(
    request: Request,
    query: str = Form(...),
    db: Session = Depends(get_db),
):
    scraper = _get_abook_scraper(db)
    if scraper is None:
        return HTMLResponse(
            '<div class="alert alert-warning">'
            '<i class="bi bi-gear-fill me-2"></i>'
            'abook.link credentials are not configured. '
            '<a href="/settings" class="alert-link">Go to Settings</a></div>'
        )

    try:
        results = scraper.search(query)
    except Exception as exc:
        logger.error("Forum search error: %s", exc)
        return HTMLResponse(
            f'<div class="alert alert-danger">'
            f'<i class="bi bi-exclamation-triangle-fill me-2"></i>'
            f"Forum search failed: {exc}</div>"
        )

    return templates.TemplateResponse(
        "partials/forum_results.html",
        {"request": request, "results": results, "query": query},
    )


@router.get("/topic/{topic_id}", response_class=HTMLResponse)
async def get_topic(
    request: Request,
    topic_id: str,
    db: Session = Depends(get_db),
):
    scraper = _get_abook_scraper(db)
    if scraper is None:
        return HTMLResponse(
            '<div class="alert alert-warning">'
            '<i class="bi bi-gear-fill me-2"></i>'
            'abook.link credentials are not configured. '
            '<a href="/settings" class="alert-link">Go to Settings</a></div>'
        )

    try:
        topic = scraper.get_topic(topic_id)
    except Exception as exc:
        logger.error("Get topic error for topic %s: %s", topic_id, exc)
        return HTMLResponse(
            f'<div class="alert alert-danger">'
            f'<i class="bi bi-exclamation-triangle-fill me-2"></i>'
            f"Failed to load topic: {exc}</div>"
        )

    return templates.TemplateResponse(
        "partials/topic_content.html",
        {
            "request": request,
            "topic": topic,
            "revealed_posts": {},
        },
    )


@router.post("/thank", response_class=HTMLResponse)
async def thank_post(
    request: Request,
    topic_id: str = Form(...),
    msg_id: str = Form(...),
    thank_href: str = Form(...),
    post_title: str = Form(default=""),
    db: Session = Depends(get_db),
):
    scraper = _get_abook_scraper(db)
    if scraper is None:
        return HTMLResponse(
            '<div class="alert alert-warning">'
            '<i class="bi bi-gear-fill me-2"></i>'
            'abook.link credentials are not configured. '
            '<a href="/settings" class="alert-link">Go to Settings</a></div>'
        )

    try:
        revealed = scraper.thank_and_get_content(topic_id, msg_id, thank_href)
    except Exception as exc:
        logger.error("Thank post error for msg %s: %s", msg_id, exc)
        return HTMLResponse(
            f'<div class="alert alert-danger">'
            f'<i class="bi bi-exclamation-triangle-fill me-2"></i>'
            f"Failed to thank post: {exc}</div>"
        )

    # Re-fetch topic to get full post list
    try:
        topic = scraper.get_topic(topic_id)
    except Exception as exc:
        logger.error("Re-fetch topic error: %s", exc)
        topic = {"topic_id": topic_id, "title": post_title, "posts": []}

    # Build a map of revealed content keyed by msg_id
    revealed_posts = {
        msg_id: {
            "search_term": revealed.get("search_term", ""),
            "password": revealed.get("password", ""),
            "raw_text": revealed.get("raw_text", ""),
        }
    }

    return templates.TemplateResponse(
        "partials/topic_content.html",
        {
            "request": request,
            "topic": topic,
            "post_title": post_title,
            "revealed_posts": revealed_posts,
        },
    )


@router.post("/nzb", response_class=HTMLResponse)
async def search_nzb(
    request: Request,
    search_term: str = Form(...),
    nzb_source: str = Form(default="nzbking"),
    password: str = Form(default=""),
    post_title: str = Form(default=""),
    topic_id: str = Form(default=""),
    msg_id: str = Form(default=""),
):
    if not search_term.strip():
        return HTMLResponse(
            '<div class="alert alert-warning">'
            '<i class="bi bi-exclamation-triangle-fill me-2"></i>'
            "No search term provided.</div>"
        )

    term = search_term.strip()

    try:
        if nzb_source == "binsearch":
            results = await asyncio.to_thread(BinsearchScraper().search, term)
            for r in results:
                r.setdefault("source", "binsearch")
        else:
            results = await asyncio.to_thread(NzbkingScraper().search, term)
            for r in results:
                r["source"] = "nzbking"
    except Exception as exc:
        source_label = "Binsearch" if nzb_source == "binsearch" else "NZBKing"
        logger.error("%s search error for '%s': %s", source_label, term, exc)
        return HTMLResponse(
            f'<div class="alert alert-danger">'
            f'<i class="bi bi-exclamation-triangle-fill me-2"></i>'
            f"{source_label} search failed: {exc}</div>"
        )

    return templates.TemplateResponse(
        "partials/nzb_results.html",
        {
            "request": request,
            "results": results,
            "search_term": search_term,
            "password": password,
            "post_title": post_title,
            "topic_id": topic_id,
            "msg_id": msg_id,
        },
    )
