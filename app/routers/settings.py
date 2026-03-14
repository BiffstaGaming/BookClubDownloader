import logging
from fastapi import APIRouter, Depends, Form, Request
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates
from sqlalchemy.orm import Session

from app.database import get_db
from app.models import Setting
from app.scrapers.abook import AbookScraper
from app.services.nzbget import NzbgetClient
from app.services.abs import AbsClient

logger = logging.getLogger(__name__)
router = APIRouter()
templates = Jinja2Templates(directory="app/templates")

SETTING_KEYS = [
    "abook_url",
    "abook_username",
    "abook_password",
    "nzbget_url",
    "nzbget_username",
    "nzbget_password",
    "nzbget_category",
    "nzbget_path_prefix",
    "local_path_prefix",
    "m4b_output_path",
    "m4b_move_template",
    "m4b_jobs",
    "m4b_bitrate",
    "abs_url",
    "abs_token",
    "abs_library_id",
]


def get_setting(db: Session, key: str, default: str = "") -> str:
    row = db.query(Setting).filter(Setting.key == key).first()
    return row.value if row and row.value is not None else default


def set_setting(db: Session, key: str, value: str):
    row = db.query(Setting).filter(Setting.key == key).first()
    if row:
        row.value = value
    else:
        db.add(Setting(key=key, value=value))
    db.commit()


@router.get("", response_class=HTMLResponse)
async def settings_page(request: Request, db: Session = Depends(get_db)):
    current = {key: get_setting(db, key) for key in SETTING_KEYS}
    # Provide a sensible default for abook_url if not set
    if not current.get("abook_url"):
        current["abook_url"] = "https://abook.link/book/index.php"
    return templates.TemplateResponse(
        "settings.html",
        {"request": request, "settings": current, "message": None, "message_type": None},
    )


@router.post("", response_class=HTMLResponse)
async def save_settings(
    request: Request,
    db: Session = Depends(get_db),
    abook_url: str = Form(default=""),
    abook_username: str = Form(default=""),
    abook_password: str = Form(default=""),
    nzbget_url: str = Form(default=""),
    nzbget_username: str = Form(default=""),
    nzbget_password: str = Form(default=""),
    nzbget_category: str = Form(default=""),
    nzbget_path_prefix: str = Form(default=""),
    local_path_prefix: str = Form(default=""),
    m4b_output_path: str = Form(default=""),
    m4b_move_template: str = Form(default=""),
    m4b_jobs: str = Form(default=""),
    m4b_bitrate: str = Form(default=""),
    abs_url: str = Form(default=""),
    abs_token: str = Form(default=""),
    abs_library_id: str = Form(default=""),
):
    values = {
        "abook_url": abook_url,
        "abook_username": abook_username,
        "abook_password": abook_password,
        "nzbget_url": nzbget_url,
        "nzbget_username": nzbget_username,
        "nzbget_password": nzbget_password,
        "nzbget_category": nzbget_category,
        "nzbget_path_prefix": nzbget_path_prefix,
        "local_path_prefix": local_path_prefix,
        "m4b_output_path": m4b_output_path,
        "m4b_move_template": m4b_move_template,
        "m4b_jobs": m4b_jobs,
        "m4b_bitrate": m4b_bitrate,
        "abs_url": abs_url,
        "abs_token": abs_token,
        "abs_library_id": abs_library_id,
    }
    for key, value in values.items():
        set_setting(db, key, value)

    logger.info("Settings saved successfully")
    current = {key: get_setting(db, key) for key in SETTING_KEYS}
    return templates.TemplateResponse(
        "settings.html",
        {
            "request": request,
            "settings": current,
            "message": "Settings saved successfully.",
            "message_type": "success",
        },
    )


@router.post("/test-nzbget", response_class=HTMLResponse)
async def test_nzbget(
    request: Request,
    nzbget_url: str = Form(default=""),
    nzbget_username: str = Form(default=""),
    nzbget_password: str = Form(default=""),
    db: Session = Depends(get_db),
):
    # Use form values if provided, fall back to DB
    if not nzbget_url:
        nzbget_url = get_setting(db, "nzbget_url")
    if not nzbget_username:
        nzbget_username = get_setting(db, "nzbget_username")
    if not nzbget_password:
        nzbget_password = get_setting(db, "nzbget_password")

    if not nzbget_url:
        return HTMLResponse(
            '<div class="alert alert-warning mb-0">NZBGet URL is not configured.</div>'
        )

    try:
        client = NzbgetClient(nzbget_url, nzbget_username, nzbget_password)
        success = client.test_connection()
        if success:
            return HTMLResponse(
                '<div class="alert alert-success mb-0">'
                '<i class="bi bi-check-circle-fill me-2"></i>Connection successful!</div>'
            )
        else:
            return HTMLResponse(
                '<div class="alert alert-danger mb-0">'
                '<i class="bi bi-x-circle-fill me-2"></i>Connection failed. '
                "Check your NZBGet URL and credentials.</div>"
            )
    except Exception as exc:
        logger.error("NZBGet connection test error: %s", exc)
        return HTMLResponse(
            f'<div class="alert alert-danger mb-0">'
            f'<i class="bi bi-x-circle-fill me-2"></i>Error: {exc}</div>'
        )


@router.post("/test-abook", response_class=HTMLResponse)
async def test_abook(
    request: Request,
    abook_url: str = Form(default=""),
    abook_username: str = Form(default=""),
    abook_password: str = Form(default=""),
    db: Session = Depends(get_db),
):
    if not abook_url:
        abook_url = get_setting(db, "abook_url")
    if not abook_username:
        abook_username = get_setting(db, "abook_username")
    if not abook_password:
        abook_password = get_setting(db, "abook_password")

    if not abook_url or not abook_username or not abook_password:
        return HTMLResponse(
            '<div class="alert alert-warning mb-0">abook.link URL, username and password are all required.</div>'
        )

    try:
        scraper = AbookScraper(abook_url, abook_username, abook_password)
        success = scraper.login()
        if success:
            return HTMLResponse(
                '<div class="alert alert-success mb-0">'
                '<i class="bi bi-check-circle-fill me-2"></i>Login successful!</div>'
            )
        else:
            return HTMLResponse(
                '<div class="alert alert-danger mb-0">'
                '<i class="bi bi-x-circle-fill me-2"></i>Login failed. '
                "Check your username and password.</div>"
            )
    except Exception as exc:
        logger.error("abook.link login test error: %s", exc)
        return HTMLResponse(
            f'<div class="alert alert-danger mb-0">'
            f'<i class="bi bi-x-circle-fill me-2"></i>Error: {exc}</div>'
        )


@router.post("/abs-libraries", response_class=HTMLResponse)
async def fetch_abs_libraries(
    request: Request,
    abs_url: str = Form(default=""),
    abs_token: str = Form(default=""),
    db: Session = Depends(get_db),
):
    if not abs_url:
        abs_url = get_setting(db, "abs_url")
    if not abs_token:
        abs_token = get_setting(db, "abs_token")

    if not abs_url or not abs_token:
        return HTMLResponse(
            '<div class="alert alert-warning mb-0">ABS URL and API token are required.</div>'
        )

    try:
        client = AbsClient(abs_url, abs_token)
        libraries = client.get_libraries()
        if not libraries:
            return HTMLResponse(
                '<div class="alert alert-warning mb-0">No libraries found.</div>'
            )
        current = get_setting(db, "abs_library_id")
        options = "".join(
            f'<option value="{lib["id"]}" {"selected" if lib["id"] == current else ""}>'
            f'{lib["name"]}</option>'
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
        return HTMLResponse(
            f'<div class="alert alert-danger mb-0">'
            f'<i class="bi bi-x-circle-fill me-2"></i>Error: {exc}</div>'
        )
