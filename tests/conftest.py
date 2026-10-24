import os
import sys
import tempfile
from pathlib import Path

import pytest

# Configure the app before it is imported: throwaway database, no background tasks.
_DB_DIR = tempfile.mkdtemp(prefix="bookclub-tests-")
os.environ["DATABASE_URL"] = "sqlite:///" + Path(_DB_DIR, "test.db").as_posix()
os.environ["BOOKCLUB_DISABLE_PIPELINE"] = "1"
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
os.chdir(Path(__file__).resolve().parents[1])  # templates/static are loaded relative to the repo root

from app.main import app  # noqa: E402
from app.database import SessionLocal  # noqa: E402
from app.models import Download, LogEntry, Setting  # noqa: E402
from app.services import notify  # noqa: E402


@pytest.fixture(autouse=True)
def clean_db(monkeypatch):
    db = SessionLocal()
    for model in (Download, LogEntry, Setting):
        db.query(model).delete()
    db.commit()
    db.close()
    sent = []
    monkeypatch.setattr(notify, "post_webhook", lambda url, payload: sent.append(payload))
    yield sent


@pytest.fixture
def db():
    session = SessionLocal()
    yield session
    session.close()


@pytest.fixture
def client():
    from fastapi.testclient import TestClient
    return TestClient(app)


@pytest.fixture
def settings(db):
    from app.settings_store import set_setting

    def _set(**values):
        for key, value in values.items():
            set_setting(db, key, value)
    return _set
