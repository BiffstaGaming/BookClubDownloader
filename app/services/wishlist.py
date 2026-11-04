"""
Wishlist and followed series: BookClub watches the forum for you.

  book    "Project Hail Mary by Andy Weir" — requested as soon as a matching topic appears
  author  "Rina Kent" — new uploads by the author (the ones already there when you
          started watching are not "new")
  series  "Royal Elite" — every book of the series you don't have yet, as soon as
          it is on the forum (also the ones already there), lowest number first

Found books are requested through the normal request flow (forum post → NZB →
NZBGet), so duplicates are still caught. With "Download automatically" off they
are listed on the Wishlist page and announced instead.
"""
import json
import logging
import re
from datetime import datetime, timedelta

from app.database import SessionLocal
from app.log_handler import log_to_db
from app.models import BookRequest, Download, WishlistItem
from app.services import book_requests, forum_cache
from app.services.abs import AbsClient, item_series
from app.services.matching import author_similarity, normalize, parse_nzb_name, ratio
from app.settings_store import get_int, get_setting

logger = logging.getLogger(__name__)

KINDS = {"book": "Book", "author": "Author", "series": "Series"}
ITEMS_PER_ROUND = 5          # forum searches per background round, to go easy on the forum
ACTIVE = ("watching", "requested", "found")


# ── reading forum topic titles ────────────────────────────────────────────────

_SERIES_SEGMENT = re.compile(r"^(?P<name>.*?\D)\s*[#,]?\s*(?:book|bk|vol\.?|volume|part)?\s*"
                             r"(?P<num>\d{1,3}(?:\.\d{1,2})?)$", re.I)
_TRAILING_WORD = re.compile(r"[\s,#-]*\b(?:book|bk|vol|volume|part|series)\s*$", re.I)
_IN_TITLE = re.compile(r"\b(?:book|bk|vol\.?|volume|#)\s*(\d{1,3}(?:\.\d{1,2})?)\b", re.I)


def number_key(value) -> str:
    """'06' → '6', '1.50' → '1.5', '' → ''."""
    text = str(value or "").strip()
    try:
        number = float(text)
    except ValueError:
        return text
    return str(int(number)) if number.is_integer() else f"{number:g}"


def parse_series_topic(topic_title: str) -> tuple[str, str]:
    """
    ("Hunter Files", "6") from "Book Club - Rob Jones - Hunter Files 06 - Title (Narrator/2021)".
    ("", "") when the title names no series number.
    """
    name = re.sub(r"\s*\([^)]*\)\s*$", "", topic_title or "").strip()
    parts = [p.strip() for p in name.split(" - ") if p.strip()]
    for segment in reversed(parts[1:-1]):
        match = _SERIES_SEGMENT.match(segment)
        if match:
            series = _TRAILING_WORD.sub("", match.group("name")).strip(" ,#-")
            if series and not series.lower().startswith("book club"):
                return series, number_key(match.group("num"))
    match = _IN_TITLE.search(parts[-1] if parts else name)
    return ("", number_key(match.group(1))) if match else ("", "")


def same_series(wanted: str, candidate: str) -> bool:
    a, b = (re.sub(r"^the\s+", "", normalize(x)) for x in (wanted, candidate))
    if not a or not b:
        return False
    if a == b or ratio(a, b) >= 0.85:
        return True
    shorter, longer = sorted((a, b), key=len)
    return len(shorter) >= 4 and f" {shorter} " in f" {longer} "


def _contains_all(needle: str, haystack: str) -> bool:
    words = normalize(needle).split()
    hay = f" {normalize(haystack)} "
    return bool(words) and all(f" {w} " in hay for w in words)


def by_author(author: str, topic_title: str) -> bool:
    _title, topic_author = parse_nzb_name(topic_title)
    return author_similarity(author, topic_author) >= 0.85 or _contains_all(author, topic_title)


# ── what you already have ─────────────────────────────────────────────────────

def owned_numbers(db, series: str) -> set[str]:
    """Book numbers of `series` in Audiobookshelf and in the download history (not failed)."""
    owned = set()
    if get_setting(db, "abs_url") and get_setting(db, "abs_token") and get_setting(db, "abs_library_id"):
        try:
            client = AbsClient(get_setting(db, "abs_url"), get_setting(db, "abs_token"))
            for result in client.search_series(get_setting(db, "abs_library_id"), series):
                for book in result.get("books") or []:
                    sequence = book.get("sequence")
                    names = item_series(book)
                    if sequence is not None and same_series(series, (result.get("series") or {}).get("name", "")):
                        owned.add(number_key(sequence))
                    for name, seq in names:
                        if seq and same_series(series, name):
                            owned.add(number_key(seq))
        except Exception as exc:
            logger.warning("Could not read series '%s' from Audiobookshelf: %s", series, exc)
    for dl in db.query(Download).filter(Download.status != "failed").order_by(Download.id.desc()).limit(2000):
        meta = dl.parsed_metadata
        if meta.get("series") and meta.get("series_part") and same_series(series, meta["series"]):
            owned.add(number_key(meta["series_part"]))
        else:
            topic_series, number = parse_series_topic(dl.post_title or "")
            if number and topic_series and same_series(series, topic_series):
                owned.add(number)
    owned.discard("")
    return owned


def series_on_forum(db, scraper, series: str, author: str = "", refresh: bool = False) -> list[dict]:
    """Forum topics of a series: [{"topic_id", "title", "number"}], lowest number first."""
    seen, found = set(), []
    for term in dict.fromkeys(t for t in (series, f"{author} {series}".strip()) if t):
        topics, _cached = forum_cache.search(db, scraper, term, refresh=refresh)
        for topic in topics:
            if topic["topic_id"] in seen:
                continue
            seen.add(topic["topic_id"])
            name, number = parse_series_topic(topic["title"])
            if not number or not same_series(series, name):
                continue
            if author and not by_author(author, topic["title"]):
                continue
            found.append({"topic_id": str(topic["topic_id"]), "title": topic["title"], "number": number})
        if found:
            break

    def sort_key(t):
        try:
            return float(t["number"]), t["title"]
        except ValueError:
            return 9999.0, t["title"]
    return sorted(found, key=sort_key)


def missing_from_series(db, scraper, series: str, author: str = "", refresh: bool = False) -> list[dict]:
    """Forum topics of the series whose number you don't have, each marked with "owned"."""
    owned = owned_numbers(db, series)
    topics = series_on_forum(db, scraper, series, author, refresh=refresh)
    return [dict(t, owned=t["number"] in owned) for t in topics]


# ── requests ─────────────────────────────────────────────────────────────────

def request_topic(db, topic_id: str, topic_title: str, requested_by: str = "") -> BookRequest:
    """A request for one known forum topic (skips the forum search)."""
    title, author = parse_nzb_name(topic_title)
    query = f"{title} by {author}" if author else (title or topic_title)
    req = book_requests.create_request(db, query, requested_by)
    req.topic_id = str(topic_id)
    db.commit()
    return req


def _save_list(values) -> str:
    return json.dumps(list(values), ensure_ascii=False)


def _sync_from_request(db, item: WishlistItem) -> None:
    """A wishlist book that was requested: done once found, watching again if it wasn't."""
    if item.status != "requested" or not item.request_id:
        return
    req = db.get(BookRequest, item.request_id)
    if req is None:
        item.status, item.message = "watching", "The request was removed — still watching"
    elif req.status == "sent":
        item.status, item.message = "done", f"Found and downloading ({req.message or req.query})"
    elif req.status == "duplicate":
        item.status, item.message = "done", f"Already in your library: {req.message}"
    elif req.status in ("not_found", "failed", "cancelled"):
        item.status = "watching"
        item.message = f"Last try didn't work ({req.message or req.status}) — still watching"


def check_item(db, scraper, item: WishlistItem) -> tuple[list[int], list[tuple[str, dict]]]:
    """Look on the forum for one wishlist entry. Returns (request ids to run, notifications)."""
    requested, notices = [], []
    seen = item.seen
    found_now = []
    first_check = item.last_checked_at is None
    who = item.requested_by or "Wishlist"
    max_requests = max(1, get_int(db, "wishlist_max_per_check", 3))

    if item.kind == "book":
        topics = book_requests._search_forum(db, scraper, item.query, refresh=True)
        ranked = [t for t in book_requests.rank_topics(item.query, topics)
                  if t["score"] >= book_requests.AUTO_PICK_SCORE and str(t["topic_id"]) not in seen]
        found_now = [{"topic_id": str(t["topic_id"]), "title": t["title"], "number": ""} for t in ranked[:1]]
    elif item.kind == "author":
        topics, _cached = forum_cache.search(db, scraper, item.query, refresh=True)
        matches = [{"topic_id": str(t["topic_id"]), "title": t["title"], "number": ""}
                   for t in topics if by_author(item.query, t["title"])]
        if first_check:
            # Watching starts now: what is already on the forum isn't news.
            item.seen_topics = _save_list(dict.fromkeys([*seen, *(m["topic_id"] for m in matches)]))
            item.message = f"Watching for new books ({len(matches)} already on the forum)"
            return requested, notices
        found_now = [m for m in matches if m["topic_id"] not in seen]
    else:  # series
        missing = missing_from_series(db, scraper, item.query, item.author or "", refresh=True)
        found_now = [m for m in missing if not m["owned"] and m["topic_id"] not in seen]
        owned = sorted({m["number"] for m in missing if m["owned"]}, key=lambda n: (len(n), n))
        if not found_now:
            item.message = (f"Up to date — you have {', '.join('#' + n for n in owned)}" if owned
                            else "Nothing new on the forum yet")

    if not found_now:
        if item.kind == "book":
            item.message = "Not on the forum yet"
        return requested, notices

    if item.auto_download:
        for topic in found_now[:max_requests]:
            req = request_topic(db, topic["topic_id"], topic["title"], who)
            requested.append(req.id)
            seen.append(topic["topic_id"])
            log_to_db("INFO", "wishlist", f"Wishlist #{item.id} '{item.query}': requesting '{topic['title']}'")
        item.seen_topics = _save_list(dict.fromkeys(seen))
        if item.kind == "book":
            item.status, item.request_id = "requested", requested[0]
            item.message = f"Found '{found_now[0]['title']}' — requesting it"
        else:
            later = len(found_now) - len(requested)
            item.message = (f"Requested {len(requested)} book(s)"
                            + (f"; {later} more next time" if later > 0 else ""))
            item.request_id = requested[-1]
        notices.append(("wishlist_found", {"title": found_now[0]["title"], "requested_by": item.requested_by or "",
                                           "message": item.message}))
    else:
        known = {t["topic_id"] for t in item.found_topics}
        new = [t for t in found_now if t["topic_id"] not in known]
        item.found = _save_list([*item.found_topics, *new])
        if item.kind == "book":
            item.status = "found"
        item.message = f"{len(item.found_topics)} book(s) found — waiting for you"
        if new:
            notices.append(("wishlist_found", {"title": new[0]["title"], "requested_by": item.requested_by or "",
                                               "message": f"{len(new)} new on the forum for '{item.query}'"}))
    return requested, notices


def check_now(item_id: int) -> tuple[list[int], list[tuple[str, dict]]]:
    """Check one entry straight away (the "Check now" button, and a new entry)."""
    db = SessionLocal()
    try:
        item = db.get(WishlistItem, item_id)
        if not item:
            return [], []
        return _check(db, item)
    finally:
        db.close()


def _check(db, item: WishlistItem) -> tuple[list[int], list[tuple[str, dict]]]:
    hours = max(1, get_int(db, "wishlist_check_hours", 12))
    item.next_check_at = datetime.utcnow() + timedelta(hours=hours)
    _sync_from_request(db, item)
    if item.status not in ("watching", "found") and item.kind == "book":
        item.last_checked_at = item.last_checked_at or datetime.utcnow()
        db.commit()
        return [], []
    scraper = book_requests._scraper(db)
    if scraper is None:
        item.message = "abook.link is not set up in Settings"
        db.commit()
        return [], []
    try:
        result = check_item(db, scraper, item)
    except Exception as exc:
        logger.warning("Wishlist #%s check failed: %s", item.id, exc)
        db.rollback()
        item = db.get(WishlistItem, item.id)
        item.message = f"Couldn't check the forum: {exc}"[:300]
        item.next_check_at = datetime.utcnow() + timedelta(hours=1)
        result = ([], [])
    item.last_checked_at = datetime.utcnow()
    db.commit()
    return result


def check_due(limit: int = ITEMS_PER_ROUND) -> tuple[list[int], list[tuple[str, dict]]]:
    """The background round: check the entries that are due, a few at a time."""
    db = SessionLocal()
    requested, notices = [], []
    try:
        for item in db.query(WishlistItem).filter(WishlistItem.status == "requested").all():
            _sync_from_request(db, item)
        db.commit()
        now = datetime.utcnow()
        due = (db.query(WishlistItem)
               .filter(WishlistItem.status.in_(ACTIVE),
                       (WishlistItem.next_check_at.is_(None)) | (WishlistItem.next_check_at <= now))
               .order_by(WishlistItem.next_check_at.is_not(None), WishlistItem.next_check_at)
               .limit(limit).all())
        for item in due:
            ids, events = _check(db, item)
            requested += ids
            notices += events
    finally:
        db.close()
    return requested, notices


def add_item(db, kind: str, query: str, author: str = "", auto_download: bool = True,
             requested_by: str = "") -> tuple[WishlistItem | None, str]:
    """(item, "") or (None, why not)."""
    kind = kind if kind in KINDS else "book"
    query = re.sub(r"\s+", " ", (query or "").strip())[:200]
    if len(query) < 2:
        return None, "Type what to watch for"
    existing = (db.query(WishlistItem)
                .filter(WishlistItem.kind == kind, WishlistItem.status.in_(ACTIVE + ("paused",))).all())
    for other in existing:
        if normalize(other.query) == normalize(query):
            return None, f"Already on your wishlist: {other.query}"
    item = WishlistItem(kind=kind, query=query, author=(author or "").strip()[:120] or None,
                        auto_download=bool(auto_download), requested_by=(requested_by or "").strip()[:80] or None,
                        status="watching", message="Checking the forum…", next_check_at=None)
    db.add(item)
    db.commit()
    log_to_db("INFO", "wishlist", f"Wishlist #{item.id}: watching {KINDS[kind].lower()} '{query}'")
    return item, ""


def take_found(db, item: WishlistItem, topic_id: str, requested_by: str = "") -> BookRequest | None:
    """"Get" on a found book: request it and take it off the found list."""
    topic = next((t for t in item.found_topics if t["topic_id"] == str(topic_id)), None)
    if topic is None:
        return None
    req = request_topic(db, topic["topic_id"], topic["title"], requested_by or item.requested_by or "Wishlist")
    item.found = _save_list([t for t in item.found_topics if t["topic_id"] != str(topic_id)])
    item.seen_topics = _save_list(dict.fromkeys([*item.seen, str(topic_id)]))
    if item.kind == "book":
        item.status, item.request_id = "requested", req.id
    item.message = f"Requested '{topic['title']}'"
    db.commit()
    return req
