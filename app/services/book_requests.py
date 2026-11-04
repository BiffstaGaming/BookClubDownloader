"""
"Find and download this book" — the unattended version of the Search page, for
Home Assistant / voice requests:

  forum search → pick the topic → Thank to reveal → NZB search → send to NZBGet

Each step that needs judgement is decided by code when the answer is clear, by
Claude (if the AI reviewer is on) when it isn't, and otherwise handed back to a
person as a short list of options (status "needs_choice").
"""
import json
import logging
import re
from datetime import datetime

from app.database import SessionLocal
from app.log_handler import log_to_db
from app.models import BookRequest, Download
from app.services import ai, forum_cache, library
from app.services.matching import author_similarity, normalize, parse_nzb_name, title_similarity
from app.services.nzb import fetch_nzb, search_nzbs, submit_to_nzbget
from app.settings_store import get_setting

logger = logging.getLogger(__name__)

# A topic this close to the request, and this far ahead of the next one, is taken without asking.
AUTO_PICK_SCORE = 0.85
AUTO_PICK_MARGIN = 0.10
AI_PICK_CONFIDENCE = 75
MAX_OPTIONS = 25  # the best few are shown first; the rest behind "Show more"

ACTIVE_STATUSES = ("pending", "searching")
FINISHED_STATUSES = ("sent", "duplicate", "not_found", "failed", "cancelled")


def split_query(query: str) -> tuple[str, str]:
    """'Project Hail Mary by Andy Weir' → ('Project Hail Mary', 'Andy Weir')."""
    text = re.sub(r"\s+", " ", (query or "").strip(" .?!"))
    text = re.sub(r"^(?:the\s+)?(?:audio\s*book|book)\s+", "", text, flags=re.I)
    match = re.match(r"(.+)\s+by\s+(.+)$", text, re.I)  # last " by ": "Murder by Death by Jane Smith"
    if match:
        return match.group(1).strip(), match.group(2).strip()
    return text, ""


def _contains_all(needle: str, haystack: str) -> bool:
    words = normalize(needle).split()
    hay = f" {normalize(haystack)} "
    return bool(words) and all(f" {w} " in hay for w in words)


def score_topic(query_title: str, query_author: str, topic_title: str) -> float:
    """0-1: how well a forum topic title matches the request."""
    t_title, t_author = parse_nzb_name(topic_title)
    title_score = max(title_similarity(query_title, t_title), title_similarity(query_title, topic_title))
    if _contains_all(query_title, topic_title):
        title_score = max(title_score, 0.9)
    if not query_author:
        return title_score
    author_score = author_similarity(query_author, t_author)
    if _contains_all(query_author, topic_title) or _contains_all(query_author.split()[-1], topic_title):
        author_score = max(author_score, 0.9)
    return title_score * 0.75 + author_score * 0.25


def rank_topics(query: str, topics: list[dict]) -> list[dict]:
    q_title, q_author = split_query(query)
    ranked = [dict(t, score=round(score_topic(q_title, q_author, t.get("title", "")), 3)) for t in topics]
    ranked.sort(key=lambda t: t["score"], reverse=True)
    return ranked


def clear_winner(ranked: list[dict]) -> bool:
    if not ranked or ranked[0]["score"] < AUTO_PICK_SCORE:
        return False
    return len(ranked) == 1 or ranked[0]["score"] - ranked[1]["score"] >= AUTO_PICK_MARGIN


def _scraper(db):
    from app.scrapers.abook import get_scraper

    base_url, username, password = (get_setting(db, k) for k in ("abook_url", "abook_username", "abook_password"))
    if not (base_url and username and password):
        return None
    return get_scraper(base_url, username, password)


class _Cancelled(Exception):
    """The request was cancelled from the UI or API while it was running."""


class NoNzbFound(Exception):
    """Neither indexer has an NZB for the search term (the post may have expired)."""


def _set(db, req: BookRequest, status: str, message: str = "", **fields):
    db.refresh(req)
    if req.status == "cancelled":
        raise _Cancelled()
    req.status = status
    req.message = message
    req.updated_at = datetime.utcnow()
    for key, value in fields.items():
        setattr(req, key, value)
    db.commit()
    log_to_db("INFO" if status not in ("failed",) else "ERROR", "request",
              f"Request #{req.id} '{req.query}': {status}{' — ' + message if message else ''}")


def _event(req: BookRequest, event: str, **fields) -> tuple[str, dict]:
    return event, {"title": fields.pop("title", req.query), "request_id": req.id,
                   "requested_by": req.requested_by or "", "message": req.message or "", **fields}


def _search_forum(db, scraper, query: str) -> list[dict]:
    """Forum search matches all words, so try the title alone before the whole request."""
    title, author = split_query(query)
    attempts = [title, f"{title} {author}".strip(), query]
    seen, results = set(), []
    for term in dict.fromkeys(a for a in attempts if a):
        found, _cached_at = forum_cache.search(db, scraper, term)
        for topic in found:
            if topic["topic_id"] not in seen:
                seen.add(topic["topic_id"])
                results.append(topic)
        if results:
            break
    return results


def _choose_topic(db, req: BookRequest, topics: list[dict]) -> tuple[str, str]:
    """(topic_id, how) when a topic can be picked without asking, else ("", reason)."""
    ranked = rank_topics(req.query, topics)
    if clear_winner(ranked):
        return ranked[0]["topic_id"], f"best forum match ({ranked[0]['score']:.0%})"
    if not ai.unavailable_reason(db, switch="ai_pick_requests"):
        try:
            pick = ai.pick_topic(db, req.query, ranked[:10])
            if pick["index"] is not None and pick["confidence"] >= AI_PICK_CONFIDENCE:
                chosen = ranked[pick["index"]]
                return chosen["topic_id"], f"picked by Claude ({pick['confidence']}%): {pick['reason']}"
            log_to_db("INFO", "request", f"Request #{req.id}: Claude couldn't pick a topic — {pick['reason']}")
        except ai.ClaudeError as exc:
            log_to_db("WARNING", "request", f"Request #{req.id}: Claude unavailable — {exc}")
    return "", "several forum topics could be it"


def _reveal(db, scraper, topic_id: str) -> tuple[dict, dict, str]:
    """
    (topic, revealed post fields, msg_id) for the first post with hidden content.
    A post revealed before comes from BookClub's own store; otherwise it is
    thanked on the forum and saved.
    """
    stored = forum_cache.topic_from_store(db, topic_id)
    if stored:
        post = stored["posts"][0]
        return stored, post["revealed"], post["msg_id"]
    with scraper.lock:
        topic = scraper.get_topic(topic_id)
    for post in topic.get("posts", []):
        if post.get("revealed") and post["revealed"].get("search_term"):
            forum_cache.save_revealed(db, topic_id, post["msg_id"], topic.get("title", ""), post["revealed"])
            return topic, post["revealed"], post["msg_id"]
        if post.get("has_hidden") and post.get("thank_href"):
            with scraper.lock:
                revealed = scraper.thank_and_get_content(topic_id, post["msg_id"], post["thank_href"])
            forum_cache.save_revealed(db, topic_id, post["msg_id"], topic.get("title", ""), revealed)
            return topic, revealed, post["msg_id"]
    return topic, {}, ""


def _find_nzb(term: str) -> dict | None:
    for source in ("nzbking", "binsearch"):
        try:
            results = search_nzbs(source, term)
        except Exception as exc:
            logger.warning("%s search for '%s' failed: %s", source, term, exc)
            continue
        if results:
            return results[0]
    return None


def send_post(db, revealed: dict, topic_id: str = "", msg_id: str = "", post_title: str = "",
              requested_by: str = None, request_id: int = None, before_send=None, force: bool = False) -> Download:
    """
    Find the NZB for a revealed post's search term, send it to NZBGet and add
    the download row. Used by requests and by the Saved page's "Send again".
    Raises NoNzbFound, or whatever fetching/sending raised.
    """
    term = (revealed.get("search_term") or "").strip()
    nzb = _find_nzb(term)
    if not nzb:
        raise NoNzbFound(f"No NZB found for '{term}' — the post may have expired")
    if before_send:
        before_send()
    source = nzb.get("source") or "nzbking"
    content = fetch_nzb(source, nzb["nzb_hash"], nzb.get("title", ""))
    safe_name = library.sanitize_filename(post_title or revealed.get("title") or nzb.get("title") or term)
    password = revealed.get("password", "")
    job_id = submit_to_nzbget(db, content, safe_name, password, force=force)
    metadata = {k: revealed.get(k) for k in ("title", "author", "series", "series_part", "narrator") if revealed.get(k)}
    dl = Download(
        post_title=post_title or revealed.get("title") or term, topic_id=topic_id or None, msg_id=msg_id or None,
        search_term=term, password=password, nzb_name=safe_name, nzb_hash=nzb["nzb_hash"], nzb_source=source,
        nzb_title=nzb.get("title", ""), nzbget_id=job_id, status="sent", requested_by=requested_by,
        request_id=request_id, download_metadata=json.dumps(metadata, ensure_ascii=False) if metadata else None,
    )
    db.add(dl)
    db.commit()
    forum_cache.mark_sent(db, term)
    log_to_db("INFO", "download", f"#{dl.id}: '{dl.post_title}' sent to NZBGet (job {job_id})", download_id=dl.id)
    return dl


def run_request(request_id: int) -> tuple[str, dict] | None:
    """
    Advance one request as far as it can go. Blocking (network); call from a thread.
    Returns (event, notification fields) for the outcome, or None if nothing happened.
    """
    from app.services.pipeline import find_duplicates  # avoid an import cycle

    db = SessionLocal()
    try:
        req = db.get(BookRequest, request_id)
        if not req or req.status not in ACTIVE_STATUSES:
            return None
        scraper = _scraper(db)
        if scraper is None:
            _set(db, req, "failed", "abook.link is not set up in BookClub's Settings")
            return _event(req, "request_failed")
        if not get_setting(db, "nzbget_url"):
            _set(db, req, "failed", "NZBGet is not set up in BookClub's Settings")
            return _event(req, "request_failed")
        _set(db, req, "searching", "Searching the forum")

        try:
            topic_id = req.topic_id
            if not topic_id:
                topics = _search_forum(db, scraper, req.query)
                if not topics:
                    _set(db, req, "not_found", f"Nothing on the forum matches '{req.query}'")
                    return _event(req, "request_not_found")
                topic_id, how = _choose_topic(db, req, topics)
                if not topic_id:
                    options = [{"option": i, "topic_id": t["topic_id"], "title": t["title"]}
                               for i, t in enumerate(rank_topics(req.query, topics)[:MAX_OPTIONS], 1)]
                    _set(db, req, "needs_choice", f"Which one? {how}", options=json.dumps(options))
                    return _event(req, "request_needs_choice", options=options)
                log_to_db("INFO", "request", f"Request #{req.id}: topic {topic_id} — {how}")
                req.topic_id = topic_id
                db.commit()

            topic, revealed, msg_id = _reveal(db, scraper, topic_id)
            term = (revealed.get("search_term") or "").strip()
            post_title = topic.get("title") or req.query
            if not term:
                _set(db, req, "failed", f"Couldn't read the NZB search term from '{post_title}' — open it on the Search page")
                return _event(req, "request_failed", title=post_title)

            book_title = revealed.get("title") or parse_nzb_name(post_title)[0]
            book_author = revealed.get("author") or ""
            if not req.force:
                duplicates = find_duplicates(db, book_title, book_author, search_term=term, include_failed=False)
                if duplicates:
                    _set(db, req, "duplicate", "; ".join(duplicates))
                    return _event(req, "request_duplicate", title=book_title, author=book_author)

            def still_wanted():
                db.refresh(req)
                if req.status == "cancelled":
                    raise _Cancelled()
            try:
                dl = send_post(db, revealed, topic_id, msg_id, post_title, req.requested_by, req.id,
                               before_send=still_wanted, force=bool(req.force))
            except NoNzbFound as exc:
                _set(db, req, "not_found", str(exc))
                return _event(req, "request_not_found", title=book_title, author=book_author)
            job_id = dl.nzbget_id
            _set(db, req, "sent", f"Downloading '{post_title}' (NZBGet job {job_id})", download_id=dl.id)
            return _event(req, "request_sent", title=book_title, author=book_author, download_id=dl.id)
        except _Cancelled:
            log_to_db("INFO", "request", f"Request #{request_id} cancelled")
            return None
        except Exception as exc:
            logger.exception("Request #%s failed", request_id)
            db.rollback()
            req = db.get(BookRequest, request_id)
            try:
                _set(db, req, "failed", f"{type(exc).__name__}: {exc}"[:500])
            except _Cancelled:
                return None
            return _event(req, "request_failed")
    finally:
        db.close()


def create_request(db, query: str, requested_by: str = "", force: bool = False) -> BookRequest:
    req = BookRequest(query=query.strip()[:300], requested_by=(requested_by or "").strip()[:80] or None,
                      status="pending", force=1 if force else 0)
    db.add(req)
    db.commit()
    log_to_db("INFO", "request", f"Request #{req.id} received: '{req.query}'"
              + (f" from {req.requested_by}" if req.requested_by else ""))
    return req


def choose(db, req: BookRequest, option: int | None = None, topic_id: str = "", force: bool = False) -> str:
    """Answer a needs_choice / duplicate request. Returns an error message, or "" when it will run."""
    if req.status not in ("needs_choice", "duplicate", "not_found", "failed"):
        return f"Request is {req.status}"
    if option is not None:
        match = next((o for o in req.parsed_options if o.get("option") == option), None)
        if not match:
            return f"There is no option {option}"
        topic_id = match["topic_id"]
    if topic_id:
        req.topic_id = str(topic_id)
    if force:
        req.force = 1
    if not req.topic_id and req.status == "needs_choice":
        return "Choose one of the options"
    req.status = "pending"
    req.message = ""
    req.updated_at = datetime.utcnow()
    db.commit()
    return ""
