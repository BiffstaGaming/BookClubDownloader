import logging
import os
from contextlib import asynccontextmanager

import asyncio

from fastapi import Depends, FastAPI, Request
from fastapi.responses import PlainTextResponse, RedirectResponse
from fastapi.staticfiles import StaticFiles

from app.database import engine, get_db, migrate_db
from app.events import bus
from app.templating import templates
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
from app.services import forum_cache  # noqa: E402
from app.database import SessionLocal  # noqa: E402
from app.routers import saved as saved_router  # noqa: E402
from app.routers import mobile as mobile_router  # noqa: E402
from app.routers import covers as covers_router  # noqa: E402
from app.routers import library as library_router  # noqa: E402
from app.routers import live as live_router  # noqa: E402
from app.routers import wishlist as wishlist_router  # noqa: E402
from app.services import prowlarr  # noqa: E402
from app.log_handler import flush as flush_logs  # noqa: E402

# Once: save the forum posts behind every download already in the history.
_db = SessionLocal()
try:
    forum_cache.backfill_from_downloads(_db)
except Exception:  # never stop the app from starting over this
    logging.getLogger("app.main").exception("Could not save posts from the download history")
finally:
    _db.close()


@asynccontextmanager
async def lifespan(_app: FastAPI):
    # Tests set BOOKCLUB_DISABLE_PIPELINE=1 to drive the pipeline by hand.
    run_pipeline = os.environ.get("BOOKCLUB_DISABLE_PIPELINE") != "1"
    bus.attach(asyncio.get_running_loop())
    if run_pipeline:
        await pipeline.start()
    yield
    bus.close()
    if run_pipeline:
        await pipeline.stop()
    await asyncio.to_thread(flush_logs, 5.0)


app = FastAPI(title="BookClub Downloader", lifespan=lifespan)

app.mount("/static", StaticFiles(directory="static"), name="static")


@app.middleware("http")
async def refuse_cross_site_changes(request: Request, call_next):
    """
    The web UI has no login, so stop other websites from driving it: browsers mark
    a request another site made with `Sec-Fetch-Site: cross-site`. Only changes are
    refused; Home Assistant's /api calls carry a token instead and send no such header.
    """
    if (request.method in ("POST", "PUT", "PATCH", "DELETE")
            and not request.url.path.startswith("/api/")
            and request.headers.get("sec-fetch-site") == "cross-site"):
        return PlainTextResponse("Cross-site request refused", status_code=403)
    return await call_next(request)


@app.middleware("http")
async def send_phones_to_the_phone_ui(request: Request, call_next):
    """A phone opening a desktop page gets the /m version (unless it chose "Desktop view")."""
    if (request.method == "GET" and request.url.path in mobile_router.PHONE_PAGES
            and "hx-request" not in request.headers and mobile_router.wants_phone_ui(request)):
        target = mobile_router.PHONE_PAGES[request.url.path]
        if request.url.path == "/" and request.url.query:
            target = "/m/search"  # a saved search link (/?q=...)
        query = f"?{request.url.query}" if request.url.query else ""
        return RedirectResponse(target + query, status_code=307)
    return await call_next(request)

app.include_router(search.router, prefix="/search", tags=["search"])
app.include_router(downloads.router, prefix="/downloads", tags=["downloads"])
app.include_router(settings_router.router, prefix="/settings", tags=["settings"])
app.include_router(logs_router.router, prefix="/logs", tags=["logs"])
app.include_router(saved_router.router, prefix="/saved", tags=["saved"])
app.include_router(mobile_router.router, prefix="/m", tags=["phone"])
app.include_router(wishlist_router.router, prefix="/wishlist", tags=["wishlist"])
app.include_router(library_router.router, prefix="/library", tags=["library"])
app.include_router(covers_router.router, prefix="/covers", tags=["covers"])
app.include_router(live_router.router, tags=["live"])
app.include_router(api_router.router, prefix="/api", tags=["api"])


@app.get("/")
def root(request: Request, db=Depends(get_db)):
    return templates.TemplateResponse("index.html", {"request": request, "prowlarr_on": prowlarr.configured(db)})
