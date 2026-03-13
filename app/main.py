from fastapi import FastAPI, Request
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates

from app.database import engine
from app import models
from app.routers import search, downloads, settings as settings_router

# Create all DB tables on startup
models.Base.metadata.create_all(bind=engine)

app = FastAPI(title="BookClub Downloader")

app.mount("/static", StaticFiles(directory="static"), name="static")
templates = Jinja2Templates(directory="app/templates")

app.include_router(search.router, prefix="/search", tags=["search"])
app.include_router(downloads.router, prefix="/downloads", tags=["downloads"])
app.include_router(settings_router.router, prefix="/settings", tags=["settings"])


@app.get("/")
async def root(request: Request):
    return templates.TemplateResponse("index.html", {"request": request})
