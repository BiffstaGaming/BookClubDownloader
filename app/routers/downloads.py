import json
import logging

from fastapi import APIRouter, Depends, Form, Request
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates
from markupsafe import escape
from sqlalchemy import and_, func, not_
from sqlalchemy.orm import Session

from app.database import get_db
from app.log_handler import log_to_db
from app.models import Download
from app.services import library
from app.services.abs import AbsClient
from app.services.matching import parse_nzb_name, rank_candidates
from app.services.nzb import NzbError, fetch_nzb, search_nzbs, submit_to_nzbget
from app.services.pipeline import (
    apply_candidate, check_input_folder, enqueue, find_duplicates, pipeline, queue_positions,
)
from app.settings_store import AUDIBLE_PROVIDERS, get_setting
from app.ui import alert, alert_html, not_configured

logger = logging.getLogger(__name__)
router = APIRouter()
templates = Jinja2Templates(directory="app/templates")

# Handlers doing network I/O are plain `def` so FastAPI runs them in its thread
# pool instead of blocking the event loop (and with it the background worker).


def _get(db: Session, download_id: int) -> Download | None:
    return db.query(Download).filter(Download.id == download_id).first()


def _rows(request: Request, db: Session, downloads: list):
    return templates.TemplateResponse(
        "partials/download_rows.html",
        {
            "request": request,
            "downloads": downloads,
            "queue_positions": queue_positions(db),
            "abs_url": get_setting(db, "abs_url").rstrip("/"),
            "running_id": pipeline.current_id,
        },
    )


def _convert_form(request: Request, db: Session, dl: Download, **extra):
    saved = dl.parsed_metadata
    defaults = dict(saved)
    defaults.setdefault("title", dl.post_title or dl.nzb_name or "")
    if not saved.get("title"):
        parsed_title, parsed_author = parse_nzb_name(dl.post_title or dl.nzb_name or "")
        defaults["title"] = parsed_title or defaults["title"]
        defaults.setdefault("author", parsed_author)
    preview = library.final_book_path(get_setting(db, "m4b_output_path"), get_setting(db, "m4b_move_template"), defaults)
    return templates.TemplateResponse(
        "partials/convert_form.html",
        {
            "request": request,
            "dl": dl,
            "saved": defaults,
            "destination_preview": preview,
            "input_problem": check_input_folder(saved.get("input_path") or dl.download_path or ""),
            **extra,
        },
    )


def _refresh(response: HTMLResponse) -> HTMLResponse:
    response.headers["HX-Trigger"] = "refreshDownloads"
    return response


# ── sending NZBs ──────────────────────────────────────────────────────────────

@router.post("/send", response_class=HTMLResponse)
def send_to_nzbget(
    request: Request,
    nzb_hash: str = Form(...),
    nzb_title: str = Form(...),
    nzb_source: str = Form(default="nzbking"),
    search_term: str = Form(default=""),
    password: str = Form(default=""),
    post_title: str = Form(default=""),
    topic_id: str = Form(default=""),
    msg_id: str = Form(default=""),
    book_title: str = Form(default=""),
    book_author: str = Form(default=""),
    book_series: str = Form(default=""),
    book_series_part: str = Form(default=""),
    book_narrator: str = Form(default=""),
    replace_id: str = Form(default=""),
    force: str = Form(default=""),
    db: Session = Depends(get_db),
):
    if not get_setting(db, "nzbget_url"):
        return not_configured("NZBGet")

    replacing = _get(db, int(replace_id)) if replace_id.isdigit() else None

    if not replacing and force != "1":
        duplicates = find_duplicates(db, book_title or post_title, book_author, nzb_hash=nzb_hash, search_term=search_term)
        if duplicates:
            resend = {
                "nzb_hash": nzb_hash, "nzb_title": nzb_title, "nzb_source": nzb_source,
                "search_term": search_term, "password": password, "post_title": post_title,
                "topic_id": topic_id, "msg_id": msg_id, "book_title": book_title,
                "book_author": book_author, "book_series": book_series,
                "book_series_part": book_series_part, "book_narrator": book_narrator, "force": "1",
            }
            form = "".join(f'<input type="hidden" name="{escape(k)}" value="{escape(v)}" />' for k, v in resend.items())
            reasons = "".join(f"<li>{escape(r)}</li>" for r in duplicates)
            return HTMLResponse(
                '<div class="alert alert-warning"><i class="bi bi-exclamation-triangle-fill me-2"></i>'
                f'<strong>Not sent — this looks like a duplicate:</strong><ul class="mb-2 mt-1">{reasons}</ul>'
                f'<form hx-post="/downloads/send" hx-target="#download-status" hx-swap="innerHTML">{form}'
                '<button type="submit" class="btn btn-sm btn-outline-warning">'
                '<i class="bi bi-send me-1"></i>Send anyway</button></form></div>'
            )

    try:
        nzb_content = fetch_nzb(nzb_source, nzb_hash, nzb_title)
    except NzbError as exc:
        return alert("danger", str(exc))
    except Exception as exc:
        logger.error("Failed to download NZB %s: %s", nzb_hash, exc)
        return alert("danger", f"Failed to download NZB: {exc}")

    safe_name = library.sanitize_filename(post_title or nzb_title or search_term or nzb_hash)
    if replacing:
        password = password or replacing.password or ""
        safe_name = replacing.nzb_name or safe_name
    try:
        job_id = submit_to_nzbget(db, nzb_content, safe_name, password)
    except Exception as exc:
        logger.error("Failed to send NZB '%s' to NZBGet: %s", safe_name, exc)
        if not replacing:
            db.add(Download(
                post_title=post_title, topic_id=topic_id, msg_id=msg_id, search_term=search_term,
                password=password, nzb_name=safe_name, nzb_hash=nzb_hash, nzb_source=nzb_source,
                nzb_title=nzb_title, nzbget_id=None, status="failed",
                error_message=f"Could not send to NZBGet: {exc}",
            ))
            db.commit()
        return alert("danger", f"Failed to send to NZBGet: {exc}")

    if replacing:
        meta = replacing.parsed_metadata
        for key in ("auto_processed", "match_confidence", "input_path"):
            meta.pop(key, None)
        replacing.set_metadata(meta)
        replacing.nzb_hash, replacing.nzb_source, replacing.nzb_title = nzb_hash, nzb_source, nzb_title
        _reset_for_new_download(replacing, job_id)
        db.commit()
        log_to_db("INFO", "download", f"#{replacing.id}: re-sent with another NZB (job {job_id})", download_id=replacing.id)
        return _refresh(alert("success", f"Sent the new NZB to NZBGet (job {job_id}). The download row has been updated."))

    initial_metadata = {}
    if book_title:       initial_metadata["title"]       = book_title
    if book_author:      initial_metadata["author"]      = book_author
    if book_series:      initial_metadata["series"]      = book_series
    if book_series_part: initial_metadata["series_part"] = book_series_part
    if book_narrator:    initial_metadata["narrator"]    = book_narrator

    record = Download(
        post_title=post_title, topic_id=topic_id, msg_id=msg_id, search_term=search_term,
        password=password, nzb_name=safe_name, nzb_hash=nzb_hash, nzb_source=nzb_source,
        nzb_title=nzb_title, nzbget_id=job_id, status="sent",
        download_metadata=json.dumps(initial_metadata, ensure_ascii=False) if initial_metadata else None,
    )
    db.add(record)
    db.commit()

    logger.info("NZB '%s' sent to NZBGet with job ID %d", safe_name, job_id)
    return alert(
        "success", f"{safe_name} sent to NZBGet! Job ID {job_id} — ",
        '<a href="/downloads" class="alert-link">View Downloads</a>',
    )


def _reset_for_new_download(dl: Download, job_id: int):
    dl.nzbget_id = job_id
    dl.status = "sent"
    dl.error_message = None
    dl.missing_polls = 0
    dl.download_progress = None
    dl.download_path = None
    dl.m4b_status = None
    dl.m4b_progress = None
    dl.abs_status = None
    dl.abs_item_id = None


@router.post("/{download_id}/retry-download", response_class=HTMLResponse)
def retry_download(request: Request, download_id: int, db: Session = Depends(get_db)):
    """Fetch the same NZB again and re-submit it (e.g. after fixing the password or disk space)."""
    dl = _get(db, download_id)
    if not dl:
        return HTMLResponse('<tr><td colspan="6">Not found.</td></tr>')
    if not dl.nzb_hash:
        dl.error_message = "No NZB recorded for this download — use 'Other NZB' instead"
        db.commit()
        return _rows(request, db, [dl])
    try:
        content = fetch_nzb(dl.nzb_source or "nzbking", dl.nzb_hash, dl.nzb_title or dl.nzb_name or "")
        job_id = submit_to_nzbget(db, content, dl.nzb_name or f"bookclub-{dl.id}", dl.password or "")
    except Exception as exc:
        dl.error_message = f"Retry failed: {exc}"
        db.commit()
        return _rows(request, db, [dl])
    meta = dl.parsed_metadata
    meta.pop("auto_processed", None)
    dl.set_metadata(meta)
    _reset_for_new_download(dl, job_id)
    db.commit()
    log_to_db("INFO", "download", f"#{dl.id}: re-sent to NZBGet (job {job_id})", download_id=dl.id)
    return _rows(request, db, [dl])


@router.get("/{download_id}/alternatives", response_class=HTMLResponse)
def alternatives(request: Request, download_id: int, db: Session = Depends(get_db)):
    """Search both indexers again so a failed download can be replaced with another NZB."""
    dl = _get(db, download_id)
    if not dl:
        return alert("danger", "Download not found.")
    term = dl.search_term or ""
    if not term:
        return alert("warning", "No search term recorded for this download — search again from the Search page.")
    results, notes = [], []
    for source in ("nzbking", "binsearch"):
        try:
            found = search_nzbs(source, term)
            results.extend(r for r in found if r.get("nzb_hash") != dl.nzb_hash)
        except Exception as exc:
            notes.append(f"{source} search failed: {exc}")
    if not results:
        notes.append("Only the NZB that already failed was found." if dl.nzb_hash else "No NZBs found.")
    meta = dl.parsed_metadata
    return templates.TemplateResponse(
        "partials/nzb_results.html",
        {
            "request": request, "results": results, "notes": notes, "duplicates": [],
            "search_term": term, "password": dl.password or "", "post_title": dl.post_title or "",
            "topic_id": dl.topic_id or "", "msg_id": dl.msg_id or "",
            "book_title": meta.get("title", ""), "book_author": meta.get("author", ""),
            "book_series": meta.get("series", ""), "book_series_part": meta.get("series_part", ""),
            "book_narrator": meta.get("narrator", ""), "replace_id": str(dl.id),
        },
    )


# ── downloads page ────────────────────────────────────────────────────────────

@router.get("", response_class=HTMLResponse)
def downloads_page(request: Request, db: Session = Depends(get_db)):
    downloads = _apply_dl_filter(db.query(Download), "active").order_by(Download.created_at.desc()).limit(5).all()
    return templates.TemplateResponse(
        "downloads.html",
        {
            "request": request,
            "downloads": downloads,
            "queue_positions": queue_positions(db),
            "abs_url": get_setting(db, "abs_url").rstrip("/"),
            "running_id": pipeline.current_id,
        },
    )


def _apply_dl_filter(q, dl_filter: str):
    """
    "active" = anything that still needs attention or is in progress. A book
    drops out once it is converted and imported (or ABS isn't used).
    NULL columns are handled explicitly — SQL NULL comparisons are never TRUE.
    """
    if dl_filter == "active":
        finished = and_(
            func.coalesce(Download.m4b_status, "") == "converted",
            func.coalesce(Download.abs_status, "imported").in_(("imported", "skipped")),
        )
        q = q.filter(not_(finished))
    return q


@router.post("/sync-status", response_class=HTMLResponse)
def sync_status(
    request: Request,
    dl_filter: str = Form(default="active"),
    dl_limit: int = Form(default=5),
    db: Session = Depends(get_db),
):
    """Render the download rows. NZBGet is polled by the background pipeline, not here."""
    q = _apply_dl_filter(db.query(Download).order_by(Download.created_at.desc()), dl_filter)
    if dl_limit > 0:
        q = q.limit(dl_limit)
    return _rows(request, db, q.all())


# ── matching & conversion ─────────────────────────────────────────────────────

@router.get("/{download_id}/convert", response_class=HTMLResponse)
def get_convert_form(request: Request, download_id: int, db: Session = Depends(get_db)):
    dl = _get(db, download_id)
    if not dl:
        return alert("danger", "Download not found.")
    if dl.status != "downloaded":
        return alert("warning", "Download must be completed before converting.")
    return _convert_form(request, db, dl)


@router.get("/{download_id}/metadata-lookup", response_class=HTMLResponse)
def metadata_lookup(
    request: Request,
    download_id: int,
    title: str = "",
    author: str = "",
    provider: str = "",
    db: Session = Depends(get_db),
):
    """Search Audible via ABS and show the candidates to pick from."""
    dl = _get(db, download_id)
    if not dl:
        return alert("danger", "Download not found.")
    abs_url, abs_token = get_setting(db, "abs_url"), get_setting(db, "abs_token")
    if not abs_url or not abs_token:
        return _convert_form(request, db, dl, lookup_error="Audiobookshelf is not configured — cannot query Audible.")

    saved = dl.parsed_metadata
    nzb_title, nzb_author = parse_nzb_name(dl.post_title or dl.nzb_name or "")
    query_title = (title or nzb_title or saved.get("title") or "").strip()
    query_author = (author if title else (author or saved.get("author") or nzb_author)).strip()
    provider = provider or get_setting(db, "audible_provider")
    try:
        log_to_db("INFO", "metadata", f"#{download_id}: searching {provider} for '{query_title}' / '{query_author}'",
                  download_id=download_id)
        results = AbsClient(abs_url, abs_token).search_books(query_title, author=query_author, provider=provider)
    except Exception as exc:
        logger.warning("metadata_lookup failed for download %d: %s", download_id, exc)
        return _convert_form(request, db, dl, lookup_error=f"Lookup failed: {exc}")

    candidates = rank_candidates(results, query_title, query_author)
    return templates.TemplateResponse(
        "partials/match_picker.html",
        {
            "request": request,
            "dl": dl,
            "candidates": candidates,
            "query_title": query_title,
            "query_author": query_author,
            "provider": provider,
            "providers": AUDIBLE_PROVIDERS,
        },
    )


@router.post("/{download_id}/apply-match", response_class=HTMLResponse)
def apply_match(request: Request, download_id: int, candidate: str = Form(...), db: Session = Depends(get_db)):
    dl = _get(db, download_id)
    if not dl:
        return alert("danger", "Download not found.")
    try:
        chosen = json.loads(candidate)
        if not isinstance(chosen, dict):
            raise ValueError("bad candidate")
    except ValueError:
        return alert("danger", "Could not read the selected match.")
    meta = dl.parsed_metadata
    for key in ("subtitle", "narrator", "series", "series_part", "year", "publisher", "description",
                "asin", "isbn", "language", "genres", "cover_url"):
        meta.pop(key, None)  # don't mix fields from a previous, different match
    apply_candidate(meta, chosen)
    dl.set_metadata(meta)
    db.commit()
    log_to_db("INFO", "metadata", f"#{download_id}: match chosen — '{meta.get('title')}' by '{meta.get('author')}'",
              download_id=download_id)
    return _refresh(_convert_form(request, db, dl, save_success="Match applied — check the details, then start the conversion."))


_FORM_FIELDS = ("title", "author", "narrator", "series", "series_part", "year", "asin", "input_path")


def _save_form(dl: Download, form: dict) -> dict:
    meta = dl.parsed_metadata
    for key in _FORM_FIELDS:
        meta[key] = (form.get(key) or "").strip()
    meta.pop("output_file", None)  # destination now comes from the library template
    dl.set_metadata(meta)
    return meta


@router.post("/{download_id}/save-metadata", response_class=HTMLResponse)
async def save_metadata(request: Request, download_id: int, db: Session = Depends(get_db)):
    dl = _get(db, download_id)
    if not dl:
        return alert("danger", "Download not found.")
    _save_form(dl, dict(await request.form()))
    db.commit()
    return _convert_form(request, db, dl, save_success="Saved.")


@router.post("/{download_id}/convert", response_class=HTMLResponse)
async def start_convert(request: Request, download_id: int, db: Session = Depends(get_db)):
    dl = _get(db, download_id)
    if not dl:
        return alert("danger", "Download not found.")
    if dl.m4b_status == "converting":
        return alert("warning", "This book is already converting.")
    meta = _save_form(dl, dict(await request.form()))
    db.commit()
    problem = check_input_folder(meta.get("input_path") or dl.download_path or "")
    if problem:
        return _convert_form(request, db, dl, lookup_error=problem)
    if not meta.get("title") or not meta.get("author"):
        return _convert_form(request, db, dl, lookup_error="Title and author are required.")

    enqueue(dl)
    db.commit()
    pipeline.queue_changed()
    position = queue_positions(db).get(dl.id, 1)
    return _refresh(alert(
        "info",
        f"{dl.display_title} is queued for conversion (position {position}). "
        "Progress shows in the table above — you can close this page.",
    ))


@router.post("/{download_id}/reset-conversion", response_class=HTMLResponse)
def reset_conversion(
    request: Request,
    download_id: int,
    view: str = "row",
    db: Session = Depends(get_db),
):
    """
    Clear a stuck/failed/queued conversion status so the user can retry.
    Returns the refreshed table row by default, or the convert form when view=form
    (used by the Reset button inside the convert panel).
    """
    dl = _get(db, download_id)
    if not dl:
        if view == "form":
            return alert("danger", f"Download #{download_id} not found.")
        return HTMLResponse('<tr><td colspan="6">Not found.</td></tr>')

    if pipeline.cancel(download_id):
        log_to_db("INFO", "conversion", f"#{download_id}: running conversion stopped by user", download_id=download_id)
    dl.m4b_status   = None
    dl.m4b_progress = None
    db.commit()
    log_to_db("INFO", "conversion", f"Conversion status manually reset for download #{download_id}", download_id=download_id)

    if view != "form":
        return _rows(request, db, [dl])
    return _convert_form(request, db, dl)


@router.post("/{download_id}/reimport", response_class=HTMLResponse)
def reimport(request: Request, download_id: int, db: Session = Depends(get_db)):
    dl = _get(db, download_id)
    if not dl:
        return HTMLResponse('<tr><td colspan="6">Not found.</td></tr>')
    if dl.m4b_status == "converted" and dl.m4b_path:
        dl.abs_status = "pending"
        dl.error_message = None
        db.commit()
        pipeline.run_soon(pipeline.abs_import, dl.id)
    return _rows(request, db, [dl])


@router.delete("/{download_id}", response_class=HTMLResponse)
def delete_download(download_id: int, db: Session = Depends(get_db)):
    dl = _get(db, download_id)
    if dl:
        pipeline.cancel(download_id)
        db.delete(dl)
        db.commit()
    return HTMLResponse("")  # HTMX swaps the row with nothing, removing it


@router.get("/{download_id}/conversion-log", response_class=HTMLResponse)
def conversion_log(download_id: int, db: Session = Depends(get_db)):
    dl = _get(db, download_id)
    if not dl or not (dl.conversion_log or dl.error_message):
        return HTMLResponse('<p class="text-muted small mb-0">No log available.</p>')
    error = alert_html("danger", dl.error_message, classes="py-2 mb-2") if dl.error_message else ""
    body = (f'<pre class="bc-raw-text mb-0" style="max-height:300px;overflow-y:auto;font-size:0.75rem;">'
            f'{escape(dl.conversion_log)}</pre>') if dl.conversion_log else ""
    return HTMLResponse(
        '<div class="bc-card"><div class="bc-card-header d-flex align-items-center gap-2">'
        f'<i class="bi bi-terminal text-accent"></i><span>Log — {escape(dl.display_title)}</span>'
        '<button type="button" class="btn bc-btn-outline btn-sm ms-auto" '
        'onclick="document.getElementById(\'convert-panel\').innerHTML=\'\'"><i class="bi bi-x-lg"></i></button>'
        f'</div><div class="bc-card-body">{error}{body}</div></div>'
    )
