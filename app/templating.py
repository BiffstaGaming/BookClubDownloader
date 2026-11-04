"""Shared Jinja2 templates for every router."""
import hashlib
from datetime import datetime
from pathlib import Path

from fastapi.templating import Jinja2Templates
from markupsafe import Markup, escape

templates = Jinja2Templates(directory="app/templates")


def _asset_version() -> str:
    """Short hash of the stylesheet, appended to its URL so browsers fetch it again after an update."""
    try:
        return hashlib.sha1(Path("static/css/app.css").read_bytes()).hexdigest()[:10]
    except OSError:
        return "dev"


def localtime(value: datetime, fmt: str = "%Y-%m-%d %H:%M") -> Markup:
    """
    A UTC timestamp the browser shows in its own timezone (base.html formats
    every <time class="bc-time">). The UTC text is only the fallback.
    """
    if not value:
        return Markup("")
    iso = value.isoformat(timespec="seconds") + "Z"
    seconds = " data-bc-seconds" if "%S" in fmt else ""
    return Markup(f'<time class="bc-time" datetime="{escape(iso)}"{seconds}>{escape(value.strftime(fmt))} UTC</time>')


templates.env.globals["asset_version"] = _asset_version()
templates.env.filters["localtime"] = localtime
