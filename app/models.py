import json
from datetime import datetime
from sqlalchemy import Boolean, Column, Integer, String, Text, DateTime, Index
from app.database import Base


class Setting(Base):
    __tablename__ = "settings"

    key = Column(String, primary_key=True, index=True)
    value = Column(String, nullable=True)


class Download(Base):
    __tablename__ = "downloads"
    # Never reuse a deleted row's id (new databases; existing ones are guarded in the pipeline).
    __table_args__ = {"sqlite_autoincrement": True}

    id = Column(Integer, primary_key=True, autoincrement=True)
    post_title = Column(String, nullable=True)
    topic_id = Column(String, nullable=True)
    msg_id = Column(String, nullable=True)
    search_term = Column(String, nullable=True, index=True)
    password = Column(String, nullable=True)
    nzb_name = Column(String, nullable=True)
    nzb_hash = Column(String, nullable=True, index=True)
    nzb_source = Column(String, nullable=True)       # "nzbking", "binsearch" or "prowlarr" — needed to retry
    nzb_title = Column(String, nullable=True)        # indexer subject — needed to retry binsearch
    nzbget_id = Column(Integer, nullable=True)
    status = Column(String, default="sent", index=True)  # sent | downloaded | failed
    error_message = Column(Text, nullable=True)      # human-readable reason for the latest failure
    missing_polls = Column(Integer, nullable=True)   # consecutive polls where NZBGet had no record
    raw_content = Column(Text, nullable=True)
    created_at = Column(DateTime, default=datetime.utcnow, index=True)
    # M4B conversion fields
    download_path = Column(Text, nullable=True)
    download_metadata = Column(Text, nullable=True)  # JSON: title/author/series/narrator/asin/paths…
    download_progress = Column(Integer, nullable=True)  # 0-100 while status == "sent"
    m4b_status = Column(String, nullable=True, index=True)  # queued | converting | converted | m4b_failed | move_failed
    m4b_progress = Column(Integer, nullable=True)
    m4b_path = Column(Text, nullable=True)
    conversion_log = Column(Text, nullable=True)
    queued_at = Column(DateTime, nullable=True)      # conversion queue order
    # Audiobookshelf import fields
    abs_status = Column(String, nullable=True, index=True)  # pending | imported | not_found | failed | skipped
    abs_item_id = Column(String, nullable=True)
    imported_at = Column(DateTime, nullable=True)    # when Audiobookshelf picked the book up
    # Who asked for it (Home Assistant requests), carried into every notification
    requested_by = Column(String, nullable=True)
    request_id = Column(Integer, nullable=True)
    # The automatic match. auto_processed = it is finished with this book (matched and
    # queued, or handed to a person); until then it may be waiting to try again.
    auto_processed = Column(Boolean, nullable=False, default=False, index=True)
    auto_attempts = Column(Integer, nullable=False, default=0)
    auto_next_try = Column(DateTime, nullable=True)
    review_reason = Column(Text, nullable=True)      # why a person is needed (shown on the Downloads page)
    # Which program downloads it: NZBGet (usenet, the default) or qBittorrent (torrents).
    client = Column(String, nullable=True)           # None / "nzbget" | "qbittorrent"
    torrent_hash = Column(String, nullable=True)
    indexer = Column(String, nullable=True)          # the indexer it came from, when not the forum

    @property
    def is_torrent(self) -> bool:
        return self.client == "qbittorrent"

    @property
    def needs_review(self) -> bool:
        """Finished downloading, not converted, and the automatic match handed it to a person."""
        return self.status == "downloaded" and self.m4b_status is None and bool(self.auto_processed)

    @property
    def parsed_metadata(self) -> dict:
        """Parsed download_metadata JSON, or empty dict."""
        if self.download_metadata:
            try:
                return json.loads(self.download_metadata)
            except Exception:
                pass
        return {}

    def set_metadata(self, data: dict):
        self.download_metadata = json.dumps(data, ensure_ascii=False)

    @property
    def display_title(self) -> str:
        meta = self.parsed_metadata
        return meta.get("title") or self.post_title or self.nzb_name or f"Download #{self.id}"


class BookRequest(Base):
    """
    "Find and download this book", usually from Home Assistant / voice. The
    pipeline searches the forum, reveals the post, finds the NZB and sends it.
    """
    __tablename__ = "book_requests"
    __table_args__ = {"sqlite_autoincrement": True}

    id = Column(Integer, primary_key=True, autoincrement=True)
    query = Column(String, nullable=False)
    requested_by = Column(String, nullable=True)
    # pending | searching | needs_choice | sent | duplicate | not_found | failed | cancelled
    status = Column(String, default="pending", index=True)
    message = Column(Text, nullable=True)
    options = Column(Text, nullable=True)            # JSON list of forum topics to choose from
    topic_id = Column(String, nullable=True)         # chosen (or picked) topic
    force = Column(Integer, default=0)               # send even if it looks like a duplicate
    download_id = Column(Integer, nullable=True)
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow)

    @property
    def parsed_options(self) -> list:
        try:
            return json.loads(self.options) if self.options else []
        except ValueError:
            return []

    def as_dict(self) -> dict:
        return {
            "id": self.id,
            "query": self.query,
            "requested_by": self.requested_by or "",
            "status": self.status,
            "message": self.message or "",
            "options": self.parsed_options,
            "topic_id": self.topic_id or "",
            "download_id": self.download_id,
            "created_at": self.created_at.isoformat(timespec="seconds") + "Z" if self.created_at else None,
            "updated_at": self.updated_at.isoformat(timespec="seconds") + "Z" if self.updated_at else None,
        }


class ForumTopic(Base):
    """Every forum topic BookClub has seen in a search — a local index of the forum."""
    __tablename__ = "forum_topics"

    topic_id = Column(String, primary_key=True)
    title = Column(String, nullable=False)
    first_seen = Column(DateTime, default=datetime.utcnow)
    last_seen = Column(DateTime, default=datetime.utcnow)


class ForumSearch(Base):
    """A forum search and the topics it returned, so a repeat search needn't ask the forum."""
    __tablename__ = "forum_searches"

    query_key = Column(String, primary_key=True)     # normalised search text
    query = Column(String, nullable=False)
    topic_ids = Column(Text, nullable=False)          # JSON list, in the forum's order
    searched_at = Column(DateTime, default=datetime.utcnow)


class RevealedPost(Base):
    """
    What a thanked post revealed — kept for good, since it never changes. Lets
    BookClub (and you) send the same book to NZBGet again without the forum.
    """
    __tablename__ = "revealed_posts"

    id = Column(Integer, primary_key=True, autoincrement=True)
    topic_id = Column(String, nullable=True, index=True)
    msg_id = Column(String, nullable=True)
    topic_title = Column(String, nullable=True)
    search_term = Column(String, nullable=False, index=True)
    password = Column(String, nullable=True)
    title = Column(String, nullable=True)
    author = Column(String, nullable=True)
    series = Column(String, nullable=True)
    series_part = Column(String, nullable=True)
    narrator = Column(String, nullable=True)
    raw_text = Column(Text, nullable=True)
    revealed_at = Column(DateTime, default=datetime.utcnow)
    times_sent = Column(Integer, default=0)
    last_sent_at = Column(DateTime, nullable=True)

    __table_args__ = (Index("ix_revealed_topic_msg", "topic_id", "msg_id", unique=True),)

    @property
    def display_title(self) -> str:
        return self.title or self.topic_title or self.search_term

    def as_revealed(self) -> dict:
        """The same shape the forum scraper returns for a revealed post."""
        return {
            "search_term": self.search_term, "password": self.password or "", "title": self.title or "",
            "author": self.author or "", "series": self.series or "", "series_part": self.series_part or "",
            "narrator": self.narrator or "", "raw_text": self.raw_text or "",
        }


class LogEntry(Base):
    __tablename__ = "logs"

    id = Column(Integer, primary_key=True, autoincrement=True)
    level = Column(String(10), nullable=False)       # DEBUG, INFO, WARNING, ERROR
    source = Column(String(100), nullable=True)      # logger name / component
    message = Column(Text, nullable=False)
    download_id = Column(Integer, nullable=True)     # linked download, if any
    created_at = Column(DateTime, default=datetime.utcnow)

    __table_args__ = (
        Index("ix_logs_level", "level"),
        Index("ix_logs_created_at", "created_at"),
        Index("ix_logs_download_id", "download_id"),
    )


class WishlistItem(Base):
    """
    Something to watch the forum for, checked every few hours:
      book    — a book that isn't on the forum yet; requested as soon as it appears
      author  — new uploads by an author
      series  — the books of a series you don't have yet (a followed series)
    """
    __tablename__ = "wishlist"
    __table_args__ = {"sqlite_autoincrement": True}

    id = Column(Integer, primary_key=True, autoincrement=True)
    kind = Column(String, nullable=False, default="book")   # book | author | series
    query = Column(String, nullable=False)                  # what to look for
    author = Column(String, nullable=True)                  # narrows a series watch
    auto_download = Column(Boolean, nullable=False, default=True)
    status = Column(String, nullable=False, default="watching", index=True)  # watching | requested | done | paused
    message = Column(Text, nullable=True)
    requested_by = Column(String, nullable=True)
    request_id = Column(Integer, nullable=True)
    seen_topics = Column(Text, nullable=True)    # JSON: forum topic ids already handled
    found = Column(Text, nullable=True)          # JSON: [{topic_id, title, number}] found, waiting for you
    last_checked_at = Column(DateTime, nullable=True)
    next_check_at = Column(DateTime, nullable=True, index=True)
    created_at = Column(DateTime, default=datetime.utcnow)

    @staticmethod
    def _load(value) -> list:
        try:
            data = json.loads(value) if value else []
        except ValueError:
            return []
        return data if isinstance(data, list) else []

    @property
    def seen(self) -> list[str]:
        return [str(t) for t in self._load(self.seen_topics)]

    @property
    def found_topics(self) -> list[dict]:
        return [t for t in self._load(self.found) if isinstance(t, dict) and t.get("topic_id")]

    def as_dict(self) -> dict:
        def iso(value):
            return value.isoformat(timespec="seconds") + "Z" if value else None
        return {
            "id": self.id, "kind": self.kind, "query": self.query, "author": self.author or "",
            "auto_download": bool(self.auto_download), "status": self.status, "message": self.message or "",
            "requested_by": self.requested_by or "", "request_id": self.request_id, "found": self.found_topics,
            "last_checked_at": iso(self.last_checked_at), "next_check_at": iso(self.next_check_at),
            "created_at": iso(self.created_at),
        }
