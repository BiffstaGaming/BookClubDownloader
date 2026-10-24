import logging
import os

from fastapi import APIRouter, Depends, Form, Request
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates
from markupsafe import escape
from sqlalchemy.orm import Session

from app.database import get_db
from app.scrapers.abook import AbookScraper
from app.services import library, notify
from app.services.abs import AbsClient
from app.services.nzbget import NzbgetClient
from app.settings_store import (  # noqa: F401 — get_setting/set_setting re-exported for older imports
    AUDIBLE_PROVIDERS, BOOL_KEYS, SECRET_KEYS, SETTING_KEYS, all_settings, get_setting, set_setting,
)
from app.ui import alert

logger = logging.getLogger(__name__)
router = APIRouter()
templates = Jinja2Templates(directory="app/templates")


def _render(request: Request, db: Session, message: str = None, message_type: str = None):
    current = all_settings(db)
    secret_set = {key: bool(current.get(key)) for key in SECRET_KEYS}
    for key in SECRET_KEYS:
        current[key] = ""  # never send saved secrets back to the browser
    return templates.TemplateResponse(
        "settings.html",
        {
            "request": request,
            "settings": current,
            "secret_set": secret_set,
            "audible_providers": AUDIBLE_PROVIDERS,
            "message": message,
            "message_type": message_type,
        },
    )


@router.get("", response_class=HTMLResponse)
def settings_page(request: Request, db: Session = Depends(get_db)):
    return _render(request, db)


@router.post("", response_class=HTMLResponse)
async def save_settings(request: Request, db: Session = Depends(get_db)):
    form = await request.form()
    for key in SETTING_KEYS:
        if key in BOOL_KEYS:
            set_setting(db, key, "1" if form.get(key) else "0")
            continue
        if key not in form:
            continue  # e.g. abs_library_id before libraries were fetched
        value = str(form.get(key, "")).strip()
        if key in SECRET_KEYS and not value:
            continue  # blank secret field = keep the saved value
        set_setting(db, key, value)

    logger.info("Settings saved")
    return _render(request, db, "Settings saved successfully.", "success")


@router.post("/test-nzbget", response_class=HTMLResponse)
def test_nzbget(
    nzbget_url: str = Form(default=""),
    nzbget_username: str = Form(default=""),
    nzbget_password: str = Form(default=""),
    db: Session = Depends(get_db),
):
    nzbget_url = nzbget_url or get_setting(db, "nzbget_url")
    nzbget_username = nzbget_username or get_setting(db, "nzbget_username")
    nzbget_password = nzbget_password or get_setting(db, "nzbget_password")
    if not nzbget_url:
        return alert("warning", "NZBGet URL is not configured.", classes="mb-0")
    try:
        if NzbgetClient(nzbget_url, nzbget_username, nzbget_password).test_connection():
            return alert("success", "Connection successful!", classes="mb-0")
        return alert("danger", "Connection failed. Check your NZBGet URL and credentials.", classes="mb-0")
    except Exception as exc:
        logger.error("NZBGet connection test error: %s", exc)
        return alert("danger", f"Error: {exc}", classes="mb-0")


@router.post("/test-abook", response_class=HTMLResponse)
def test_abook(
    abook_url: str = Form(default=""),
    abook_username: str = Form(default=""),
    abook_password: str = Form(default=""),
    db: Session = Depends(get_db),
):
    abook_url = abook_url or get_setting(db, "abook_url")
    abook_username = abook_username or get_setting(db, "abook_username")
    abook_password = abook_password or get_setting(db, "abook_password")
    if not abook_url or not abook_username or not abook_password:
        return alert("warning", "abook.link URL, username and password are all required.", classes="mb-0")
    try:
        if AbookScraper(abook_url, abook_username, abook_password).login():
            return alert("success", "Login successful!", classes="mb-0")
        return alert("danger", "Login failed. Check your username and password.", classes="mb-0")
    except Exception as exc:
        logger.error("abook.link login test error: %s", exc)
        return alert("danger", f"Error: {exc}", classes="mb-0")


@router.post("/abs-libraries", response_class=HTMLResponse)
def fetch_abs_libraries(
    abs_url: str = Form(default=""),
    abs_token: str = Form(default=""),
    db: Session = Depends(get_db),
):
    abs_url = abs_url or get_setting(db, "abs_url")
    abs_token = abs_token or get_setting(db, "abs_token")
    if not abs_url or not abs_token:
        return alert("warning", "ABS URL and API key are required.", classes="mb-0")
    try:
        libraries = AbsClient(abs_url, abs_token).get_libraries()
        if not libraries:
            return alert("warning", "No libraries found.", classes="mb-0")
        current = get_setting(db, "abs_library_id")
        options = "".join(
            f'<option value="{escape(lib["id"])}" {"selected" if lib["id"] == current else ""}>'
            f'{escape(lib["name"])} ({escape(lib.get("mediaType", "book"))})</option>'
            for lib in libraries
        )
        return HTMLResponse(
            f'<select class="form-select bc-input" id="abs_library_id" name="abs_library_id">'
            f'{options}</select>'
            f'<div class="form-text text-muted mt-1">'
            f'<i class="bi bi-check-circle-fill text-success me-1"></i>'
            f'Connected — select your audiobook library then save.</div>'
        )
    except Exception as exc:
        logger.error("ABS library fetch error: %s", exc)
        return alert("danger", f"Error: {exc}", classes="mb-0")


@router.post("/preview-template", response_class=HTMLResponse)
def preview_template(m4b_move_template: str = Form(default=""), m4b_output_path: str = Form(default="")):
    sample_series = {"title": "The Way of Kings", "author": "Brandon Sanderson", "series": "The Stormlight Archive",
                     "series_part": "1", "narrator": "Michael Kramer", "year": "2010"}
    sample_single = {"title": "Project Hail Mary", "author": "Andy Weir", "narrator": "Ray Porter", "year": "2021"}
    output = m4b_output_path or "/m4b"
    rows = "".join(
        f'<div><code class="bc-code">{escape(library.final_book_path(output, m4b_move_template, meta))}</code></div>'
        for meta in (sample_series, sample_single)
    )
    warning = library.template_layout_warning(m4b_move_template)
    warn_html = (f'<div class="text-warning small mt-1"><i class="bi bi-exclamation-triangle me-1"></i>'
                 f'{escape(warning)}</div>') if warning else ""
    return HTMLResponse(f'<div class="small text-muted">Example result:</div>{rows}{warn_html}')


@router.post("/check-paths", response_class=HTMLResponse)
def check_paths(
    local_path_prefix: str = Form(default=""),
    nzbget_path_prefix: str = Form(default=""),
    m4b_output_path: str = Form(default=""),
    m4b_move_template: str = Form(default=""),
):
    checks = []
    downloads = local_path_prefix or nzbget_path_prefix
    if downloads:
        checks.append(("Downloads folder", downloads, False))
    checks.append(("M4B output / working folder", m4b_output_path or "/m4b", True))
    template = m4b_move_template.strip()
    if template.startswith("/"):
        fixed = template.split("[", 1)[0].rstrip("/") or "/"
        checks.append(("Library root (from template)", fixed, True))

    rows = []
    for label, path, need_write in checks:
        if not os.path.isdir(path):
            state, cls = "does not exist inside the container — check your volume mounts", "text-danger"
        elif need_write and not os.access(path, os.W_OK):
            state, cls = "exists but is not writable — check PUID/PGID and folder permissions", "text-danger"
        else:
            state, cls = "OK", "text-success"
        rows.append(f'<div class="small"><strong>{escape(label)}</strong> <code class="bc-code">{escape(path)}</code> '
                    f'<span class="{cls}">{escape(state)}</span></div>')
    return HTMLResponse("".join(rows))


@router.post("/test-ha", response_class=HTMLResponse)
def test_ha(ha_webhook_url: str = Form(default=""), db: Session = Depends(get_db)):
    url = ha_webhook_url or get_setting(db, "ha_webhook_url")
    if not url:
        return alert("warning", "Enter the Home Assistant webhook URL first.", classes="mb-0")
    try:
        notify.post_webhook(url, notify.build_payload(
            "test", title="BookClub test", message="If you can see this, BookClub can reach Home Assistant."))
        return alert("success", "Sent — check your Home Assistant automation's trace.", classes="mb-0")
    except Exception as exc:
        return alert("danger", f"Webhook failed: {exc}", classes="mb-0")
