from datetime import datetime
from sqlalchemy import Column, Integer, String, Text, DateTime
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
