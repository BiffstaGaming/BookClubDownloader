import logging
import threading
import time

from fastapi import APIRouter, Depends, Form, Request
from fastapi.responses import HTMLResponse
from sqlalchemy.orm import Session

from app.database import get_db
from app.scrapers.abook import get_scraper
from app.services import forum_cache, prowlarr, releases
from app.services.nzb import rank_nzbs, search_nzbs
from app.services.pipeline import find_duplicates
from app.settings_store import get_setting
from app.templating import templates
from app.ui import alert, alert_html, not_configured

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
    refresh: str = Form(default=""),
    db: Session = Depends(get_db),
):
    """Forum search — a repeat of a recent search is answered from BookClub's own copy."""
    scraper = _get_abook_scraper(db)
    if scraper is None:
        return not_configured("abook.link")
    try:
        results, cached_at = forum_cache.search(db, scraper, query, refresh=refresh == "1")
    except Exception as exc:
        logger.error("Forum search error: %s", exc)
        return alert("danger", f"Forum search failed: {exc}")

    return templates.TemplateResponse(
        "partials/forum_results.html",
        {"request": request, "results": results, "query": query, "cached_at": cached_at,
         "revealed_ids": forum_cache.revealed_topic_ids(db, [r["topic_id"] for r in results])},
    )


@router.get("/local", response_class=HTMLResponse)
def local_topics(request: Request, query: str = "", db: Session = Depends(get_db)):
    """Topics BookClub has already seen that match what's being typed — no forum request."""
    matches = forum_cache.local_matches(db, query)
    return templates.TemplateResponse(
        "partials/local_topics.html",
        {"request": request, "matches": matches,
         "revealed_ids": forum_cache.revealed_topic_ids(db, [m["topic_id"] for m in matches])},
    )


@router.get("/topic/{topic_id}", response_class=HTMLResponse)
def get_topic(
    request: Request,
    topic_id: str,
    refresh: str = "",
    db: Session = Depends(get_db),
):
    # A post revealed before is shown from BookClub's own store — no forum visit.
    stored = None if refresh == "1" else forum_cache.topic_from_store(db, topic_id)
    if stored:
        topic = stored
    else:
        scraper = _get_abook_scraper(db)
        if scraper is None:
            return not_configured("abook.link")
        try:
            with scraper.lock:
                topic = scraper.get_topic(topic_id)
        except Exception as exc:
            logger.error("Get topic error for topic %s: %s", topic_id, exc)
            return alert("danger", f"Failed to load topic: {exc}")
        for post in topic.get("posts", []):
            if post.get("revealed"):
                forum_cache.save_revealed(db, topic_id, post["msg_id"], topic.get("title", ""), post["revealed"])

    return templates.TemplateResponse(
        "partials/topic_content.html",
        {
            "request": request,
            "topic": topic,
            "topic_url": _topic_url(db, topic_id),
            "revealed_posts": {},
            "auto_search": False,
            "from_store": bool(stored),
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
        # The thank worked — still show what it revealed (and run the NZB search).
        topic = {"topic_id": topic_id, "title": post_title,
                 "posts": [{"msg_id": msg_id, "subject": "", "author": "", "thank_href": "", "has_hidden": False,
                            "revealed": revealed}]}
    if not any(str(p.get("msg_id")) == str(msg_id) for p in topic.get("posts", [])):
        topic.setdefault("posts", []).insert(0, {"msg_id": msg_id, "subject": "", "author": "", "thank_href": "",
                                                 "has_hidden": False, "revealed": revealed})

    forum_cache.save_revealed(db, topic_id, msg_id, topic.get("title") or post_title, revealed)

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
            results = rank_nzbs(search_nzbs(source, term))
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


# ── other indexers (Prowlarr) ─────────────────────────────────────────────────

# Results are kept here, not sent to the browser: their download links carry the Prowlarr API key.
RELEASE_TTL_SECONDS = 3600
_releases: dict[str, tuple[float, dict]] = {}
_releases_lock = threading.Lock()


def _remember(results: list[dict]) -> None:
    now = time.monotonic()
    with _releases_lock:
        for key in [k for k, (at, _r) in _releases.items() if now - at > RELEASE_TTL_SECONDS]:
            del _releases[key]
        for r in results:
            _releases[r["guid"]] = (now, r)


@router.post("/indexers", response_class=HTMLResponse)
def search_indexers(request: Request, query: str = Form(default=""), db: Session = Depends(get_db)):
    query = query.strip()
    if not prowlarr.configured(db):
        return alert("info", "Connect Prowlarr in Settings to search your usenet and torrent indexers too.")
    if len(query) < 2:
        return alert("warning", "Type a title (and author).")
    try:
        results = releases.search(db, query)
    except Exception as exc:
        logger.warning("Prowlarr search for '%s' failed: %s", query, exc)
        return alert("danger", f"Prowlarr search failed: {exc}")
    results = [r for r in results if r["guid"]][:60]
    _remember(results)
    from app.services.book_requests import split_query
    title, author = split_query(query)
    return templates.TemplateResponse("partials/release_results.html", {
        "request": request, "results": results, "query": query, "book_title": title, "book_author": author,
        "blocked": {r["guid"]: releases.can_send(db, r) for r in results},
    })


@router.post("/release", response_class=HTMLResponse)
def send_release(guid: str = Form(...), book_title: str = Form(default=""), book_author: str = Form(default=""),
                 force: str = Form(default=""), db: Session = Depends(get_db)):
    with _releases_lock:
        entry = _releases.get(guid)
    if not entry:
        return alert("warning", "That result has expired — search again.")
    release = entry[1]
    if force != "1":
        duplicates = find_duplicates(db, book_title or release["title"], book_author, nzb_hash=release["guid"][:500])
        if duplicates:
            reasons = "; ".join(duplicates)
            send_anyway = ('<button type="button" class="btn btn-sm btn-outline-warning ms-2" hx-post="/search/release" '
                           'hx-target="closest .bc-release-status" hx-vals=\'{"force": "1"}\' '
                           'hx-include="closest form">Send anyway</button>')
            return HTMLResponse(alert_html("warning", f"Not sent — you may already have it: {reasons}.", send_anyway))
    try:
        dl = releases.send_release(db, release, {"title": book_title, "author": book_author})
    except Exception as exc:
        logger.warning("Sending '%s' failed: %s", release["title"], exc)
        return alert("danger", f"Not sent: {exc}")
    where = "qBittorrent" if dl.is_torrent else "NZBGet"
    return alert("success", f"Sent to {where} — ", '<a href="/downloads" class="alert-link">View Downloads</a>')
