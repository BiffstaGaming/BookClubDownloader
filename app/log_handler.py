"""
DBLogHandler — a Python logging.Handler that persists log records to SQLite.

All loggers whose name starts with "app." are automatically captured.
To avoid infinite recursion, SQLAlchemy's own engine/pool logs are excluded.
"""
import logging
from datetime import datetime


# Short, human-friendly source label from a full logger name
def _short_source(name: str) -> str:
    return name.replace("app.routers.", "").replace("app.scrapers.", "").replace("app.services.", "").replace("app.", "")


class DBLogHandler(logging.Handler):
    """Writes log records from app.* loggers into the LogEntry table."""

    # Guard against re-entrant calls (e.g. if SQLAlchemy logs during our write)
    _writing = False

    def emit(self, record: logging.LogRecord):
        if DBLogHandler._writing:
            return
        if not record.name.startswith("app."):
            return

        DBLogHandler._writing = True
        try:
            from app.database import SessionLocal
            from app.models import LogEntry

            db = SessionLocal()
            try:
                entry = LogEntry(
                    level=record.levelname,
                    source=_short_source(record.name),
                    message=self.format(record),
                    created_at=datetime.utcnow(),
                )
                db.add(entry)
                db.commit()
            except Exception:
                pass
            finally:
                db.close()
        finally:
            DBLogHandler._writing = False


def log_to_db(level: str, source: str, message: str, download_id: int = None):
    """
    Explicit helper for writing a structured log entry — use this when you
    have a download_id to associate or want to log content that isn't going
    through the Python logging system (e.g. raw subprocess output).
    """
    try:
        from app.database import SessionLocal
        from app.models import LogEntry

        db = SessionLocal()
        try:
            entry = LogEntry(
                level=level.upper(),
                source=source,
                message=message,
                download_id=download_id,
                created_at=datetime.utcnow(),
            )
            db.add(entry)
            db.commit()
        except Exception:
            pass
        finally:
            db.close()
    except Exception:
        pass
