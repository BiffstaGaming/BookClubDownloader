"""
The Logs page's storage. Log lines from every `app.*` logger, and the explicit
log_to_db() calls, are queued and written to SQLite by one background thread in
batches — so writing a log line never waits for the database, and the event loop
that runs the pipeline never does either.

flush() waits until everything queued so far is written (tests, shutdown).
BOOKCLUB_SYNC_LOGS=1 writes each line straight away instead.
"""
import atexit
import logging
import os
import queue
import sys
import threading
import time
from datetime import datetime, timezone

BATCH_SECONDS = 0.5     # how long the writer gathers lines before one commit
BATCH_MAX = 500


def _short_source(name: str) -> str:
    """Short, human-friendly source label from a full logger name."""
    return name.replace("app.routers.", "").replace("app.scrapers.", "").replace("app.services.", "").replace("app.", "")


class _Writer:
    def __init__(self):
        self.queue: queue.SimpleQueue = queue.SimpleQueue()
        self.thread: threading.Thread | None = None
        self.lock = threading.Lock()

    @property
    def synchronous(self) -> bool:
        return os.environ.get("BOOKCLUB_SYNC_LOGS") == "1"

    def put(self, entry: dict):
        if self.synchronous:
            self._write([entry])
            return
        self._ensure_thread()
        self.queue.put(entry)

    def flush(self, timeout: float = 10.0) -> bool:
        """Block until every line queued before this call is in the database."""
        if self.thread is None or not self.thread.is_alive():
            return True
        done = threading.Event()
        self.queue.put(done)
        return done.wait(timeout)

    def _ensure_thread(self):
        if self.thread is not None and self.thread.is_alive():
            return
        with self.lock:
            if self.thread is None or not self.thread.is_alive():
                self.thread = threading.Thread(target=self._run, name="bookclub-log-writer", daemon=True)
                self.thread.start()

    def _run(self):
        while True:
            items = [self.queue.get()]
            deadline = time.monotonic() + BATCH_SECONDS
            while len(items) < BATCH_MAX and not isinstance(items[-1], threading.Event):
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    break
                try:
                    items.append(self.queue.get(timeout=remaining))
                except queue.Empty:
                    break
            self._write([i for i in items if isinstance(i, dict)])
            for item in items:
                if isinstance(item, threading.Event):
                    item.set()

    @staticmethod
    def _write(entries: list[dict]):
        if not entries:
            return
        try:
            from app.database import SessionLocal
            from app.models import LogEntry

            db = SessionLocal()
            try:
                try:
                    db.add_all([LogEntry(**entry) for entry in entries])
                    db.commit()
                except Exception:
                    db.rollback()
                    if len(entries) == 1:
                        raise
                    # One bad line must not lose the whole batch: write them one by one.
                    for entry in entries:
                        try:
                            db.add(LogEntry(**entry))
                            db.commit()
                        except Exception as exc:
                            db.rollback()
                            print(f"BookClub: could not write a log line: {exc}", file=sys.stderr)
            finally:
                db.close()
        except Exception as exc:
            # Never through `logging`: that would queue another line about this one.
            print(f"BookClub: could not write {len(entries)} log line(s): {exc}", file=sys.stderr)


_writer = _Writer()
atexit.register(_writer.flush, 5.0)


def flush(timeout: float = 10.0) -> bool:
    return _writer.flush(timeout)


class DBLogHandler(logging.Handler):
    """Queues log records from app.* loggers for the Logs page."""

    def emit(self, record: logging.LogRecord):
        if not record.name.startswith("app."):
            return
        try:
            message = self.format(record)
        except Exception:
            self.handleError(record)
            return
        _writer.put({"level": record.levelname, "source": _short_source(record.name), "message": message,
                     "download_id": getattr(record, "download_id", None),
                     "created_at": datetime.fromtimestamp(record.created, timezone.utc).replace(tzinfo=None)})


def log_to_db(level: str, source: str, message: str, download_id: int = None):
    """
    Explicit helper for writing a structured log entry — use this when you
    have a download_id to associate or want to log content that isn't going
    through the Python logging system (e.g. raw subprocess output).
    """
    try:
        _writer.put({"level": level.upper(), "source": source, "message": message,
                     "download_id": download_id, "created_at": datetime.utcnow()})
    except Exception:
        pass
