import json
from datetime import datetime
from sqlalchemy import Column, Integer, String, Text, DateTime, Index
from app.database import Base


class Setting(Base):
    __tablename__ = "settings"

    key = Column(String, primary_key=True, index=True)
    value = Column(String, nullable=True)


class Download(Base):
    __tablename__ = "downloads"

    id = Column(Integer, primary_key=True, autoincrement=True)
    post_title = Column(String, nullable=True)
    topic_id = Column(String, nullable=True)
    msg_id = Column(String, nullable=True)
    search_term = Column(String, nullable=True)
    password = Column(String, nullable=True)
    nzb_name = Column(String, nullable=True)
    nzb_hash = Column(String, nullable=True)
    nzb_source = Column(String, nullable=True)       # "nzbking" or "binsearch" — needed to retry
    nzb_title = Column(String, nullable=True)        # indexer subject — needed to retry binsearch
    nzbget_id = Column(Integer, nullable=True)
    status = Column(String, default="sent")          # sent | downloaded | failed
    error_message = Column(Text, nullable=True)      # human-readable reason for the latest failure
    missing_polls = Column(Integer, nullable=True)   # consecutive polls where NZBGet had no record
    raw_content = Column(Text, nullable=True)
    created_at = Column(DateTime, default=datetime.utcnow)
    # M4B conversion fields
    download_path = Column(Text, nullable=True)
    download_metadata = Column(Text, nullable=True)  # JSON: title/author/series/narrator/asin/paths…
    download_progress = Column(Integer, nullable=True)  # 0-100 while status == "sent"
    m4b_status = Column(String, nullable=True)       # queued | converting | converted | m4b_failed | move_failed
    m4b_progress = Column(Integer, nullable=True)
    m4b_path = Column(Text, nullable=True)
    conversion_log = Column(Text, nullable=True)
    queued_at = Column(DateTime, nullable=True)      # conversion queue order
    # Audiobookshelf import fields
    abs_status = Column(String, nullable=True)       # pending | imported | not_found | failed | skipped
    abs_item_id = Column(String, nullable=True)

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
    )
