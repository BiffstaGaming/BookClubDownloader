"""
Local memory of the abook.link forum, so BookClub asks the forum less:

  topics    every topic seen in a search (title + topic #) — a local index
  searches  recent searches and what they returned, reused for a few hours
            (Settings → abook.link → "Reuse search results for"); new uploads
            appear once that time is up, or straight away with "Refresh"
  revealed  what a thanked post revealed (search term, password, details) —
            kept for good; it never changes, and it is what's needed to send
            the same book to NZBGet again (the Saved page)
"""
import json
import logging
from datetime import datetime, timedelta

from sqlalchemy.orm import Session

from app.models import Download, ForumSearch, ForumTopic, RevealedPost
from app.services.matching import normalize, parse_nzb_name
from app.settings_store import get_int, get_setting, set_setting

logger = logging.getLogger(__name__)


# ── searches ──────────────────────────────────────────────────────────────────

def record_topics(db: Session, topics: list[dict]) -> None:
    now = datetime.utcnow()
    for t in topics:
        if not t.get("topic_id") or not t.get("title"):
            continue
        row = db.get(ForumTopic, str(t["topic_id"]))
        if row:
            row.title, row.last_seen = t["title"], now
        else:
            db.add(ForumTopic(topic_id=str(t["topic_id"]), title=t["title"], first_seen=now, last_seen=now))
    db.commit()


def cached_search(db: Session, query: str) -> tuple[list[dict], datetime] | None:
    """(topics, when searched) if this search was done recently enough, else None."""
    hours = get_int(db, "forum_cache_hours", 12)
    row = db.get(ForumSearch, normalize(query))
    if hours <= 0 or not row or datetime.utcnow() - row.searched_at > timedelta(hours=hours):
        return None
    try:
        ids = json.loads(row.topic_ids)
    except ValueError:
        return None
    topics = {t.topic_id: t for t in db.query(ForumTopic).filter(ForumTopic.topic_id.in_(ids)).all()}
    return [{"topic_id": i, "title": topics[i].title} for i in ids if i in topics], row.searched_at


def search(db: Session, scraper, query: str, refresh: bool = False) -> tuple[list[dict], datetime | None]:
    """
    Forum search through the cache. Returns (topics, cached_at) — cached_at is
    None when the forum was just asked.
    """
    if not refresh:
        cached = cached_search(db, query)
        if cached is not None:
            return cached
    with scraper.lock:
        results = scraper.search(query)
    record_topics(db, results)
    key = normalize(query)
    row = db.get(ForumSearch, key) or ForumSearch(query_key=key)
    row.query = query
    row.topic_ids = json.dumps([str(t["topic_id"]) for t in results])
    row.searched_at = datetime.utcnow()
    db.add(row)
    db.commit()
    return results, None


def local_matches(db: Session, query: str, limit: int = 8) -> list[dict]:
    """Topics already seen whose title contains every word typed."""
    words = normalize(query).split()
    if not words or len(" ".join(words)) < 3:
        return []
    q = db.query(ForumTopic)
    for word in words[:6]:
        q = q.filter(ForumTopic.title.ilike(f"%{word}%"))
    rows = q.order_by(ForumTopic.last_seen.desc()).limit(limit).all()
    return [{"topic_id": r.topic_id, "title": r.title} for r in rows]


# ── revealed posts ────────────────────────────────────────────────────────────

def save_revealed(db: Session, topic_id: str, msg_id: str, topic_title: str, revealed: dict) -> RevealedPost | None:
    """Keep what a thanked post revealed. Ignored when there's no search term."""
    term = (revealed or {}).get("search_term", "").strip()
    if not term:
        return None
    row = None
    if topic_id and msg_id:
        row = db.query(RevealedPost).filter(RevealedPost.topic_id == str(topic_id),
                                            RevealedPost.msg_id == str(msg_id)).first()
    if row is None:
        row = db.query(RevealedPost).filter(RevealedPost.search_term == term).first()
    if row is None:
        row = RevealedPost(search_term=term, revealed_at=datetime.utcnow(), times_sent=0)
        db.add(row)
    row.topic_id = str(topic_id) if topic_id else row.topic_id
    row.msg_id = str(msg_id) if msg_id else row.msg_id
    row.topic_title = topic_title or row.topic_title
    row.search_term = term
    for key in ("password", "title", "author", "series", "series_part", "narrator"):
        value = (revealed.get(key) or "").strip()
        if value or not getattr(row, key):
            setattr(row, key, value or getattr(row, key))
    if revealed.get("raw_text"):
        row.raw_text = revealed["raw_text"][:4000]
    db.commit()
    return row


def revealed_for_topic(db: Session, topic_id: str) -> list[RevealedPost]:
    return (db.query(RevealedPost).filter(RevealedPost.topic_id == str(topic_id))
            .order_by(RevealedPost.id).all())


def topic_from_store(db: Session, topic_id: str) -> dict | None:
    """A topic shaped like scraper.get_topic(), built from saved posts — no forum request."""
    posts = revealed_for_topic(db, topic_id)
    if not posts:
        return None
    topic = db.get(ForumTopic, str(topic_id))
    return {
        "topic_id": str(topic_id),
        "title": posts[0].topic_title or (topic.title if topic else f"Topic {topic_id}"),
        "posts": [{"msg_id": p.msg_id or "", "subject": "", "author": "", "thank_href": "", "has_hidden": False,
                   "revealed": p.as_revealed()} for p in posts],
    }


def revealed_topic_ids(db: Session, topic_ids: list[str]) -> set[str]:
    if not topic_ids:
        return set()
    rows = db.query(RevealedPost.topic_id).filter(RevealedPost.topic_id.in_([str(t) for t in topic_ids])).all()
    return {r[0] for r in rows}


def mark_sent(db: Session, search_term: str) -> None:
    row = db.query(RevealedPost).filter(RevealedPost.search_term == (search_term or "").strip()).first()
    if row:
        row.times_sent = (row.times_sent or 0) + 1
        row.last_sent_at = datetime.utcnow()
        db.commit()


def backfill_from_downloads(db: Session) -> int:
    """
    Once, on the first start of this version: every download already in the
    history knows its search term and password, so those posts are saved too.
    """
    if get_setting(db, "revealed_backfilled") == "1":
        return 0
    added = 0
    for dl in db.query(Download).filter(Download.search_term.isnot(None)).order_by(Download.id).all():
        term = (dl.search_term or "").strip()
        if not term or db.query(RevealedPost).filter(RevealedPost.search_term == term).first():
            continue
        if dl.topic_id and dl.msg_id and db.query(RevealedPost).filter(
                RevealedPost.topic_id == dl.topic_id, RevealedPost.msg_id == dl.msg_id).first():
            continue
        meta = dl.parsed_metadata
        forum_title = meta.get("forum_title") or meta.get("title") or parse_nzb_name(dl.post_title or "")[0]
        db.add(RevealedPost(
            topic_id=dl.topic_id or None, msg_id=dl.msg_id or None, topic_title=dl.post_title,
            search_term=term, password=dl.password or "", title=forum_title,
            author=meta.get("forum_author") or meta.get("author") or "",
            series=meta.get("series", ""), series_part=meta.get("series_part", ""),
            narrator=meta.get("forum_narrator") or meta.get("narrator") or "",
            revealed_at=dl.created_at or datetime.utcnow(), times_sent=1, last_sent_at=dl.created_at,
        ))
        db.flush()
        added += 1
    set_setting(db, "revealed_backfilled", "1")  # commits
    if added:
        logger.info("Saved %d revealed post(s) from the download history", added)
    return added
