import logging
import os
from contextlib import asynccontextmanager

from fastapi import FastAPI, Request
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates

from app.database import engine, migrate_db
from app import models
from app.routers import search, downloads, settings as settings_router
from app.routers import logs as logs_router
from app.routers import api as api_router

# Create all DB tables on startup, then apply any missing column migrations
models.Base.metadata.create_all(bind=engine)
migrate_db()

# Register the DB log handler so all app.* logger calls are persisted
from app.log_handler import DBLogHandler
_db_handler = DBLogHandler()
_db_handler.setFormatter(logging.Formatter("%(message)s"))
_db_handler.setLevel(logging.DEBUG)
logging.getLogger("app").addHandler(_db_handler)
logging.getLogger("app").setLevel(logging.DEBUG)

from app.services.pipeline import pipeline  # noqa: E402 — needs the tables above


@asynccontextmanager
async def lifespan(_app: FastAPI):
    # Tests set BOOKCLUB_DISABLE_PIPELINE=1 to drive the pipeline by hand.
    run_pipeline = os.environ.get("BOOKCLUB_DISABLE_PIPELINE") != "1"
    if run_pipeline:
        await pipeline.start()
    yield
    if run_pipeline:
        await pipeline.stop()


app = FastAPI(title="BookClub Downloader", lifespan=lifespan)

app.mount("/static", StaticFiles(directory="static"), name="static")
templates = Jinja2Templates(directory="app/templates")

app.include_router(search.router, prefix="/search", tags=["search"])
app.include_router(downloads.router, prefix="/downloads", tags=["downloads"])
app.include_router(settings_router.router, prefix="/settings", tags=["settings"])
app.include_router(logs_router.router, prefix="/logs", tags=["logs"])
app.include_router(api_router.router, prefix="/api", tags=["api"])


@app.get("/")
async def root(request: Request):
    return templates.TemplateResponse("index.html", {"request": request})
