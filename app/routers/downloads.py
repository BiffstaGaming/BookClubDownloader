import json
import logging
from datetime import datetime, timedelta

from fastapi import APIRouter, Depends, Form, Request
from fastapi.responses import HTMLResponse
from markupsafe import escape
from sqlalchemy import and_, func, not_, or_
from sqlalchemy.orm import Session

from app.database import get_db
from app.log_handler import log_to_db
from app.models import BookRequest, Download, LogEntry
from app.services import book_requests, forum_cache, library, prowlarr, releases
from app.services.abs import AbsClient
from app.services.matching import parse_nzb_name, rank_candidates
from app.services.nzb import NzbError, fetch_nzb, find_nzbs, nzbget_client, submit_to_nzbget
from app.services.pipeline import (
    apply_candidate, check_input_folder, enqueue, find_duplicates, mark_handled_by_hand, pipeline, queue_positions,
    reset_auto_state, reset_for_new_download,
)
from app.settings_store import AUDIBLE_PROVIDERS, get_setting
from app.templating import templates
from app.ui import alert, alert_html, not_configured

logger = logging.getLogger(__name__)
router = APIRouter()

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
            "encode_wait": pipeline.encode_wait,
            "encode_note": pipeline.encode_note,
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

    replacing = _get(db, int(replace_id)) if replace_id.isdecimal() else None

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
                '<div class="alert alert-warning alert-dismissible" role="alert">'
                '<button type="button" class="btn-close btn-close-sm" data-bs-dismiss="alert" aria-label="Close"></button>'
                '<i class="bi bi-exclamation-triangle-fill me-2"></i>'
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
        # The replacement's own search word and password (they may have been changed under "Fix & retry").
        safe_name = replacing.nzb_name or safe_name
    try:
        # A deliberate re-send ("Send anyway", another NZB) must not be dropped by NZBGet's duplicate check.
        job_id = submit_to_nzbget(db, nzb_content, safe_name, password, force=bool(replacing) or force == "1")
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
        reset_auto_state(replacing)
        meta = replacing.parsed_metadata
        for key in ("match_confidence", "input_path", "nzb_retries"):
            meta.pop(key, None)
        replacing.set_metadata(meta)
        replacing.nzb_hash, replacing.nzb_source, replacing.nzb_title = nzb_hash, nzb_source, nzb_title
        replacing.search_term = search_term or replacing.search_term
        replacing.password = password
        replacing.client, replacing.torrent_hash = None, None
        reset_for_new_download(replacing, job_id)
        db.commit()
        forum_cache.mark_sent(db, replacing.search_term or search_term)
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
    forum_cache.mark_sent(db, search_term)

    logger.info("NZB '%s' sent to NZBGet with job ID %d", safe_name, job_id)
    return alert(
        "success", f"{safe_name} sent to NZBGet! Job ID {job_id} — ",
        '<a href="/downloads" class="alert-link">View Downloads</a>',
    )


@router.post("/{download_id}/retry-download", response_class=HTMLResponse)
def retry_download(request: Request, download_id: int, db: Session = Depends(get_db)):
    """Fetch the same NZB again and re-submit it (e.g. after fixing the password or disk space)."""
    dl = _get(db, download_id)
    if not dl:
        return HTMLResponse('<tr><td colspan="6">Not found.</td></tr>')
    error = _resend_same_nzb(db, dl)
    if error:
        dl.error_message = error
        db.commit()
    return _rows(request, db, [dl])


def _resend_same_nzb(db: Session, dl: Download) -> str:
    """Fetch the download's NZB again and send it to NZBGet with its (current) password. "" or why not."""
    if dl.is_torrent:
        return "Torrents can't be re-sent — use Fix & retry to pick another result"
    if not dl.nzb_hash:
        return "No NZB recorded for this download — use Fix & retry to find one"
    try:
        content = fetch_nzb(dl.nzb_source or "nzbking", dl.nzb_hash, dl.nzb_title or dl.nzb_name or "",
                            url=dl.parsed_metadata.get("download_url", ""))
        job_id = submit_to_nzbget(db, content, dl.nzb_name or f"bookclub-{dl.id}", dl.password or "", force=True)
    except Exception as exc:
        return f"Retry failed: {exc}"
    reset_auto_state(dl)
    meta = dl.parsed_metadata
    meta.pop("nzb_retries", None)  # a fresh start for the automatic "try another NZB"
    dl.set_metadata(meta)
    reset_for_new_download(dl, job_id)
    db.commit()
    log_to_db("INFO", "download", f"#{dl.id}: re-sent to NZBGet (job {job_id})", download_id=dl.id)
    return ""


# ── "Fix & retry": every step of a failed download, editable ──────────────────

_GONE = "This download isn't failed any more — close this panel."


def _failed(db: Session, download_id: int) -> Download | None:
    dl = _get(db, download_id)
    return dl if dl and dl.status == "failed" else None


def _keep_fields(dl: Download, search_term: str, password: str) -> None:
    """The search word and password as typed in the panel (an empty password is a real choice)."""
    dl.search_term = search_term.strip() or dl.search_term
    dl.password = password.strip()


def _fix_panel(request: Request, db: Session, dl: Download, notice: str = "", error: str = ""):
    meta = dl.parsed_metadata
    title, author = parse_nzb_name(dl.post_title or dl.nzb_name or "")
    query = " ".join(x for x in (meta.get("title") or title, meta.get("author") or author) if x)
    posts = forum_cache.revealed_for_topic(db, dl.topic_id) if dl.topic_id else []
    return templates.TemplateResponse("partials/fix_download.html", {
        "request": request, "dl": dl, "notice": notice, "error": error, "posts": posts,
        "forum_url": get_setting(db, "abook_url").rstrip("/"),
        "forum_ready": book_requests._scraper(db) is not None,
        "nzbget_ready": bool(get_setting(db, "nzbget_url")),
        "prowlarr_ready": prowlarr.configured(db),
        "can_resend": bool(dl.nzb_hash) and not dl.is_torrent
                      and (dl.nzb_source != "prowlarr" or bool(meta.get("download_url"))),
        "indexer_query": query or dl.search_term or "",
    })


@router.get("/{download_id}/fix", response_class=HTMLResponse)
def fix_panel(request: Request, download_id: int, db: Session = Depends(get_db)):
    dl = _failed(db, download_id)
    return _fix_panel(request, db, dl) if dl else alert("info", _GONE)


@router.post("/{download_id}/fix/save", response_class=HTMLResponse)
def fix_save(request: Request, download_id: int, search_term: str = Form(default=""),
             password: str = Form(default=""), db: Session = Depends(get_db)):
    dl = _failed(db, download_id)
    if not dl:
        return alert("info", _GONE)
    _keep_fields(dl, search_term, password)
    db.commit()
    log_to_db("INFO", "download", f"#{dl.id}: search word / password changed by hand", download_id=dl.id)
    return _refresh(_fix_panel(request, db, dl, notice="Saved."))


@router.post("/{download_id}/fix/unpack", response_class=HTMLResponse)
def fix_unpack(request: Request, download_id: int, search_term: str = Form(default=""),
               password: str = Form(default=""), db: Session = Depends(get_db)):
    """Wrong password: NZBGet unpacks the files it already downloaded again, with the new password."""
    dl = _failed(db, download_id)
    if not dl:
        return alert("info", _GONE)
    _keep_fields(dl, search_term, password)
    db.commit()
    if not dl.nzbget_id or dl.is_torrent:
        return _fix_panel(request, db, dl, error="NZBGet has no job for this download — download it again instead.")
    try:
        nzbget_client(db).unpack_again(dl.nzbget_id, dl.password or "")
    except Exception as exc:
        return _fix_panel(request, db, dl, error=f"NZBGet couldn't unpack it again: {exc}. Download it again instead.")
    reset_auto_state(dl)
    reset_for_new_download(dl, dl.nzbget_id)
    db.commit()
    log_to_db("INFO", "download", f"#{dl.id}: NZBGet unpacking job {dl.nzbget_id} again with a new password",
              download_id=dl.id)
    return _refresh(alert("success", "NZBGet is unpacking the files it already has, with the new password. "
                                     "The download row follows it."))


@router.post("/{download_id}/fix/resend", response_class=HTMLResponse)
def fix_resend(request: Request, download_id: int, search_term: str = Form(default=""),
               password: str = Form(default=""), db: Session = Depends(get_db)):
    dl = _failed(db, download_id)
    if not dl:
        return alert("info", _GONE)
    _keep_fields(dl, search_term, password)
    db.commit()
    error = _resend_same_nzb(db, dl)
    if error:
        return _fix_panel(request, db, dl, error=error)
    with_password = f" with password {dl.password}" if dl.password else " without a password"
    return _refresh(alert("success", f"Downloading the same NZB again (NZBGet job {dl.nzbget_id}){with_password}."))


@router.post("/{download_id}/fix/nzbs", response_class=HTMLResponse)
def fix_find_nzbs(request: Request, download_id: int, search_term: str = Form(default=""),
                  password: str = Form(default=""), db: Session = Depends(get_db)):
    """NZBKing and binsearch, searched with the (edited) search word."""
    dl = _failed(db, download_id)
    if not dl:
        return alert("info", _GONE)
    term = search_term.strip()
    if not term:
        return alert("warning", "Type the search word first.")
    found, notes = find_nzbs(term, every_source=True)
    meta = dl.parsed_metadata
    return templates.TemplateResponse("partials/nzb_results.html", {
        "request": request, "results": found, "notes": notes, "duplicates": [], "failed_hash": dl.nzb_hash or "",
        "search_term": term, "password": password.strip(), "post_title": dl.post_title or "",
        "topic_id": dl.topic_id or "", "msg_id": dl.msg_id or "",
        "book_title": meta.get("title", ""), "book_author": meta.get("author", ""),
        "book_series": meta.get("series", ""), "book_series_part": meta.get("series_part", ""),
        "book_narrator": meta.get("narrator", ""), "replace_id": str(dl.id),
    })


@router.post("/{download_id}/fix/indexers", response_class=HTMLResponse)
def fix_indexers(request: Request, download_id: int, query: str = Form(default=""), db: Session = Depends(get_db)):
    """Prowlarr, searched with the (edited) title; the chosen result replaces the failed one."""
    from app.routers.search import _remember

    dl = _failed(db, download_id)
    if not dl:
        return alert("info", _GONE)
    query = query.strip()
    if len(query) < 2:
        return alert("warning", "Type a title (and author).")
    try:
        results = [r for r in releases.search(db, query) if r["guid"]][:60]
    except Exception as exc:
        return alert("danger", f"Prowlarr search failed: {exc}")
    _remember(results)
    title, author = book_requests.split_query(query)
    return templates.TemplateResponse("partials/release_results.html", {
        "request": request, "results": results, "query": query, "book_title": title, "book_author": author,
        "blocked": {r["guid"]: releases.can_send(db, r) for r in results}, "replace_id": str(dl.id),
    })


@router.post("/{download_id}/fix/reread", response_class=HTMLResponse)
def fix_reread(request: Request, download_id: int, db: Session = Depends(get_db)):
    """Read the forum topic again: every post with hidden content, revealed and saved."""
    dl = _failed(db, download_id)
    if not dl:
        return alert("info", _GONE)
    scraper = book_requests._scraper(db)
    if scraper is None or not dl.topic_id:
        return _fix_panel(request, db, dl, error="abook.link isn't set up in Settings, or this download has no forum post.")
    try:
        with scraper.lock:
            topic = scraper.get_topic(dl.topic_id)
        for post in topic.get("posts", []):
            revealed = post.get("revealed")
            if not (revealed and revealed.get("search_term")) and post.get("has_hidden") and post.get("thank_href"):
                with scraper.lock:
                    revealed = scraper.thank_and_get_content(dl.topic_id, post["msg_id"], post["thank_href"])
            if revealed and revealed.get("search_term"):
                forum_cache.save_revealed(db, dl.topic_id, post["msg_id"], topic.get("title", ""), revealed)
    except Exception as exc:
        return _fix_panel(request, db, dl, error=f"Couldn't read the forum post: {exc}")
    count = len(forum_cache.revealed_for_topic(db, dl.topic_id))
    log_to_db("INFO", "download", f"#{dl.id}: forum post read again ({count} post(s) with a search word)",
              download_id=dl.id)
    return _fix_panel(request, db, dl, notice=f"Read the forum post again: {count} post(s) with a search word.")


# ── requests ("find and download this book") ─────────────────────────────────

REQUEST_FOUND_MINUTES = 15  # how long a found ("sent") request stays in the list


def _request_rows(request: Request, db: Session, notice: str | None = ""):
    """
    The requests still worth showing: anything searching or waiting for you, and
    a found one for a while (its download is followed on the Downloads page).
    Cancelled ones are gone straight away. `notice` replaces the message above
    the list; None (background refresh) leaves it alone.
    """
    found_cutoff = datetime.utcnow() - timedelta(minutes=REQUEST_FOUND_MINUTES)
    rows = (db.query(BookRequest)
            .filter(BookRequest.status != "cancelled",
                    or_(BookRequest.status != "sent", BookRequest.updated_at >= found_cutoff))
            .order_by(BookRequest.id.desc()).limit(8).all())
    return templates.TemplateResponse(
        "partials/request_rows.html", {"request": request, "requests": rows, "notice": notice})


@router.get("/requests", response_class=HTMLResponse)
def request_rows(request: Request, db: Session = Depends(get_db)):
    return _request_rows(request, db, notice=None)


@router.post("/requests", response_class=HTMLResponse)
def new_request(request: Request, query: str = Form(...), db: Session = Depends(get_db)):
    if len(query.strip()) < 2:
        return _request_rows(request, db, alert_html("warning", "Type the book's title (and author if you know it)."))
    req = book_requests.create_request(db, query, "")
    pipeline.run_soon(pipeline.process_request, req.id)
    return _request_rows(request, db)


@router.post("/requests/{request_id}/choose", response_class=HTMLResponse)
def choose_request(request: Request, request_id: int, option: str = Form(default=""), force: str = Form(default=""),
                   db: Session = Depends(get_db)):
    req = db.get(BookRequest, request_id)
    if not req:
        return _request_rows(request, db)
    error = book_requests.choose(db, req, int(option) if option.isdecimal() else None, force=force == "1")
    if error:
        return _request_rows(request, db, alert_html("warning", error))
    pipeline.run_soon(pipeline.process_request, req.id)
    return _request_rows(request, db)


@router.post("/requests/{request_id}/retry", response_class=HTMLResponse)
def retry_request(request: Request, request_id: int, db: Session = Depends(get_db)):
    req = db.get(BookRequest, request_id)
    if req and req.status in ("not_found", "failed"):
        error = book_requests.choose(db, req)
        if not error:
            pipeline.run_soon(pipeline.process_request, req.id)
    return _request_rows(request, db)


@router.delete("/requests/{request_id}", response_class=HTMLResponse)
def remove_request(request: Request, request_id: int, db: Session = Depends(get_db)):
    req = db.get(BookRequest, request_id)
    if req:
        if req.status in book_requests.ACTIVE_STATUSES:
            req.status = "cancelled"  # still running: let it see the cancellation
        else:
            db.delete(req)
        db.commit()
    return _request_rows(request, db)


# ── downloads page ────────────────────────────────────────────────────────────

@router.get("", response_class=HTMLResponse)
def downloads_page(request: Request, db: Session = Depends(get_db)):
    downloads = _apply_dl_filter(db.query(Download), "active").order_by(Download.created_at.desc()).limit(25).all()
    return templates.TemplateResponse(
        "downloads.html",
        {
            "request": request,
            "downloads": downloads,
            "queue_positions": queue_positions(db),
            "abs_url": get_setting(db, "abs_url").rstrip("/"),
            "running_id": pipeline.current_id,
            "encode_wait": pipeline.encode_wait,
            "encode_note": pipeline.encode_note,
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
    elif dl_filter == "attention":
        # Waiting for you: failures, and finished downloads the automatic match handed over.
        waiting = and_(Download.status == "downloaded", Download.m4b_status.is_(None),
                       Download.auto_processed == True)  # noqa: E712 — SQL comparison
        q = q.filter(or_(
            Download.status == "failed",
            Download.m4b_status.in_(("m4b_failed", "move_failed")),
            Download.abs_status.in_(("not_found", "failed")),
            waiting,
        ))
    return q


@router.post("/sync-status", response_class=HTMLResponse)
def sync_status(
    request: Request,
    dl_filter: str = Form(default="active"),
    dl_limit: int = Form(default=25),
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

    preferred_language = get_setting(db, "preferred_language")
    candidates = rank_candidates(results, query_title, query_author, preferred_language=preferred_language)
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
            "preferred_language": preferred_language,
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
    mark_handled_by_hand(dl)
    meta["match_method"] = f"you ({meta.get('match_confidence')}% match)"
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
    meta["edited_by_hand"] = True  # the details are yours now, not the automatic match's best guess
    mark_handled_by_hand(dl)  # an automatic match still running must not overwrite this
    dl.set_metadata(meta)
    return meta


def _book_form(
    title: str = Form(default=""),
    author: str = Form(default=""),
    narrator: str = Form(default=""),
    series: str = Form(default=""),
    series_part: str = Form(default=""),
    year: str = Form(default=""),
    asin: str = Form(default=""),
    input_path: str = Form(default=""),
) -> dict:
    return {"title": title, "author": author, "narrator": narrator, "series": series,
            "series_part": series_part, "year": year, "asin": asin, "input_path": input_path}


# Plain `def`: these walk the input folder on disk, which can be slow on a NAS.
@router.post("/{download_id}/save-metadata", response_class=HTMLResponse)
def save_metadata(request: Request, download_id: int, form: dict = Depends(_book_form),
                  db: Session = Depends(get_db)):
    dl = _get(db, download_id)
    if not dl:
        return alert("danger", "Download not found.")
    _save_form(dl, form)
    db.commit()
    return _convert_form(request, db, dl, save_success="Saved.")


@router.post("/{download_id}/convert", response_class=HTMLResponse)
def start_convert(request: Request, download_id: int, form: dict = Depends(_book_form),
                  db: Session = Depends(get_db)):
    dl = _get(db, download_id)
    if not dl:
        return alert("danger", "Download not found.")
    if dl.m4b_status == "converting":
        return alert("warning", "This book is already converting.")
    meta = _save_form(dl, form)
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
        # Its log goes too: SQLite can reuse the id, and a new book shouldn't inherit old entries.
        db.query(LogEntry).filter(LogEntry.download_id == download_id).delete()
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
        '<button type="button" class="btn bc-btn-outline btn-sm ms-auto" title="Close" aria-label="Close" '
        'onclick="document.getElementById(\'convert-panel\').innerHTML=\'\'"><i class="bi bi-x-lg"></i></button>'
        f'</div><div class="bc-card-body">{error}{body}</div></div>'
    )
