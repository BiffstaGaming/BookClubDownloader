import json
import logging
import os
from datetime import datetime

from sqlalchemy import create_engine, event, text
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker

logger = logging.getLogger("app.database")

DATABASE_URL = os.environ.get("DATABASE_URL", "sqlite:////app/data/bookclub.db")

# For local development (non-Docker), fall back to a local data folder
if DATABASE_URL == "sqlite:////app/data/bookclub.db" and not os.path.exists("/app/data"):
    os.makedirs("data", exist_ok=True)
    DATABASE_URL = "sqlite:///data/bookclub.db"


def _data_dir() -> str:
    """The folder next to the database, for BookClub's own files (e.g. cached covers)."""
    configured = os.environ.get("BOOKCLUB_DATA_DIR")
    if configured:
        return configured
    if DATABASE_URL.startswith("sqlite:///"):
        folder = os.path.dirname(DATABASE_URL[len("sqlite:///"):])
        if folder:
            return folder
    return "data"


DATA_DIR = _data_dir()

engine = create_engine(
    DATABASE_URL,
    connect_args={"check_same_thread": False, "timeout": 30} if DATABASE_URL.startswith("sqlite") else {},
)

if DATABASE_URL.startswith("sqlite"):
    @event.listens_for(engine, "connect")
    def _sqlite_pragmas(dbapi_conn, _record):
        # WAL lets the web UI read while the background worker writes progress.
        cur = dbapi_conn.cursor()
        cur.execute("PRAGMA journal_mode=WAL")
        cur.execute("PRAGMA synchronous=NORMAL")
        cur.close()

SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

Base = declarative_base()


def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


# Columns added after the first release. New databases get them from create_all;
# older ones get them here (adding a column that exists already just fails quietly).
NEW_COLUMNS = [
    ("downloads", "download_path", "TEXT"),
    ("downloads", "download_metadata", "TEXT"),
    ("downloads", "download_progress", "INTEGER"),
    ("downloads", "m4b_status", "TEXT"),
    ("downloads", "m4b_progress", "INTEGER"),
    ("downloads", "m4b_path", "TEXT"),
    ("downloads", "conversion_log", "TEXT"),
    ("downloads", "nzb_source", "TEXT"),
    ("downloads", "nzb_title", "TEXT"),
    ("downloads", "error_message", "TEXT"),
    ("downloads", "missing_polls", "INTEGER"),
    ("downloads", "queued_at", "DATETIME"),
    ("downloads", "abs_status", "TEXT"),
    ("downloads", "abs_item_id", "TEXT"),
    ("downloads", "imported_at", "DATETIME"),
    ("downloads", "requested_by", "TEXT"),
    ("downloads", "request_id", "INTEGER"),
    ("downloads", "auto_processed", "BOOLEAN NOT NULL DEFAULT 0"),
    ("downloads", "auto_attempts", "INTEGER NOT NULL DEFAULT 0"),
    ("downloads", "auto_next_try", "DATETIME"),
    ("downloads", "review_reason", "TEXT"),
    ("downloads", "client", "TEXT"),
    ("downloads", "torrent_hash", "TEXT"),
    ("downloads", "indexer", "TEXT"),
]

# Indexes for the columns the pages and the background worker filter on. The names
# are the ones SQLAlchemy gives `index=True` columns, so new databases match.
INDEXES = [
    ("ix_downloads_status", "downloads", "status"),
    ("ix_downloads_m4b_status", "downloads", "m4b_status"),
    ("ix_downloads_abs_status", "downloads", "abs_status"),
    ("ix_downloads_search_term", "downloads", "search_term"),
    ("ix_downloads_nzb_hash", "downloads", "nzb_hash"),
    ("ix_downloads_created_at", "downloads", "created_at"),
    ("ix_downloads_auto_processed", "downloads", "auto_processed"),
    ("ix_book_requests_status", "book_requests", "status"),
    ("ix_logs_download_id", "logs", "download_id"),
]

# The automatic match's state used to live in the JSON metadata; now it has columns.
_AUTO_KEYS = ("auto_processed", "auto_attempts", "auto_next_try", "review_reason")
_SQLITE_DATETIME = "%Y-%m-%d %H:%M:%S.%f"  # how SQLAlchemy stores DateTime in SQLite


def _create_indexes(conn):
    for name, table, column in INDEXES:
        conn.execute(text(f"CREATE INDEX IF NOT EXISTS {name} ON {table} ({column})"))


def _auto_state_to_columns(conn):
    rows = conn.execute(text(
        "SELECT id, download_metadata FROM downloads WHERE download_metadata LIKE '%\"auto_%' "
        "OR download_metadata LIKE '%\"review_reason\"%'")).fetchall()
    for row_id, raw in rows:
        try:
            meta = json.loads(raw) if raw else {}
        except ValueError:
            continue
        if not isinstance(meta, dict) or not any(key in meta for key in _AUTO_KEYS):
            continue
        next_try = None
        if meta.get("auto_next_try"):
            try:
                next_try = datetime.fromisoformat(str(meta["auto_next_try"])).strftime(_SQLITE_DATETIME)
            except ValueError:
                next_try = None
        try:
            attempts = int(meta.get("auto_attempts") or 0)
        except (TypeError, ValueError):
            attempts = 0
        conn.execute(
            text("UPDATE downloads SET auto_processed = :done, auto_attempts = :attempts, auto_next_try = :next_try, "
                 "review_reason = :reason, download_metadata = :meta WHERE id = :id"),
            {"done": 1 if meta.get("auto_processed") else 0, "attempts": attempts, "next_try": next_try,
             "reason": meta.get("review_reason") or None, "id": row_id,
             "meta": json.dumps({k: v for k, v in meta.items() if k not in _AUTO_KEYS}, ensure_ascii=False)},
        )


# Numbered, run once each, in order; PRAGMA user_version remembers how far a database got.
MIGRATIONS = [
    (1, "indexes on the columns pages filter by", _create_indexes),
    (2, "automatic-match state moved from JSON into columns", _auto_state_to_columns),
]


def migrate_db():
    """Bring an existing database up to date (safe to run on every startup)."""
    with engine.connect() as conn:
        for table, column, col_type in NEW_COLUMNS:
            try:
                conn.execute(text(f"ALTER TABLE {table} ADD COLUMN {column} {col_type}"))
                conn.commit()
            except Exception:
                conn.rollback()  # Column already exists — safe to ignore

    if not DATABASE_URL.startswith("sqlite"):
        return
    with engine.connect() as conn:
        version = conn.execute(text("PRAGMA user_version")).scalar() or 0
        for number, description, migrate in MIGRATIONS:
            if number <= version:
                continue
            migrate(conn)
            conn.execute(text(f"PRAGMA user_version = {int(number)}"))
            conn.commit()
            logger.info("Database migration %d applied: %s", number, description)
