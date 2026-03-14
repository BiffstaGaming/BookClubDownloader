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
    nzbget_id = Column(Integer, nullable=True)
    status = Column(String, default="sent")
    raw_content = Column(Text, nullable=True)
    created_at = Column(DateTime, default=datetime.utcnow)
    # M4B conversion fields
    download_path = Column(Text, nullable=True)
    download_metadata = Column(Text, nullable=True)  # JSON: title/author/series/paths
    download_progress = Column(Integer, nullable=True)  # 0-100 while status == "sent"
    m4b_status = Column(String, nullable=True)
    m4b_progress = Column(Integer, nullable=True)
    m4b_path = Column(Text, nullable=True)
    conversion_log = Column(Text, nullable=True)

    @property
    def metadata(self) -> dict:
        """Parsed download_metadata JSON, or empty dict."""
        if self.download_metadata:
            try:
                return json.loads(self.download_metadata)
            except Exception:
                pass
        return {}


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
