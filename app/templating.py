"""Shared Jinja2 templates for every router."""
import hashlib
from datetime import datetime
from pathlib import Path

from fastapi.templating import Jinja2Templates
from markupsafe import Markup, escape

TEMPLATE_DIR = Path("app/templates")


def is_mobile(request) -> bool:
    """Requests from the phone UI (/m) carry this header on every htmx call."""
    return request is not None and request.headers.get("x-bc-ui") == "mobile"


class BookClubTemplates(Jinja2Templates):
    """
    Same routes, two looks: when the phone UI asks for a fragment and a phone
    version of it exists (app/templates/mobile/<name>), that one is rendered.
    """

    def TemplateResponse(self, *args, **kwargs):
        if args and isinstance(args[0], str):
            context = args[1] if len(args) > 1 else kwargs.get("context") or {}
            if is_mobile(context.get("request")) and (TEMPLATE_DIR / "mobile" / args[0]).is_file():
                args = ("mobile/" + args[0], *args[1:])
        return super().TemplateResponse(*args, **kwargs)


templates = BookClubTemplates(directory=str(TEMPLATE_DIR))


def _asset_version() -> str:
    """Short hash of the stylesheets, appended to their URLs so browsers fetch them again after an update."""
    try:
        data = b"".join(Path(f).read_bytes() for f in ("static/css/app.css", "static/css/mobile.css", "static/js/bookclub.js")
                        if Path(f).is_file())
        return hashlib.sha1(data).hexdigest()[:10]
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
