import os
from sqlalchemy import create_engine
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
