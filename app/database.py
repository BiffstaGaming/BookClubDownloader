import os
from sqlalchemy import create_engine, text
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker

DATABASE_URL = os.environ.get("DATABASE_URL", "sqlite:////app/data/bookclub.db")

# For local development (non-Docker), fall back to a local data folder
if DATABASE_URL == "sqlite:////app/data/bookclub.db" and not os.path.exists("/app/data"):
    os.makedirs("data", exist_ok=True)
    DATABASE_URL = "sqlite:///data/bookclub.db"

engine = create_engine(
    DATABASE_URL,
    connect_args={"check_same_thread": False} if DATABASE_URL.startswith("sqlite") else {},
)

SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

Base = declarative_base()


def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


def migrate_db():
    """Add any missing columns to existing tables (safe to run on every startup)."""
    new_columns = [
        ("downloads", "download_path", "TEXT"),
        ("downloads", "m4b_status", "TEXT"),
        ("downloads", "m4b_progress", "INTEGER"),
        ("downloads", "m4b_path", "TEXT"),
        ("downloads", "conversion_log", "TEXT"),
    ]
    with engine.connect() as conn:
        for table, column, col_type in new_columns:
            try:
                conn.execute(text(f"ALTER TABLE {table} ADD COLUMN {column} {col_type}"))
                conn.commit()
            except Exception:
                pass  # Column already exists — safe to ignore
