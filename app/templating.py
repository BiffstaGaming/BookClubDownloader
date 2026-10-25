"""Shared Jinja2 templates for every router."""
import hashlib
from pathlib import Path

from fastapi.templating import Jinja2Templates

templates = Jinja2Templates(directory="app/templates")


def _asset_version() -> str:
    """Short hash of the stylesheet, appended to its URL so browsers fetch it again after an update."""
    try:
        return hashlib.sha1(Path("static/css/app.css").read_bytes()).hexdigest()[:10]
    except OSError:
        return "dev"


templates.env.globals["asset_version"] = _asset_version()
