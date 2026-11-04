"""
JSON endpoints for Home Assistant and container health checks.

Open (read-only):  GET /api/health, GET /api/status
Need the API token (Settings → Home Assistant), sent as `Authorization: Bearer <token>`:
  POST /api/requests                 {"query": "Project Hail Mary by Andy Weir", "requested_by": "janet"}
  GET  /api/requests                 recent requests
  GET  /api/requests/{id}
  POST /api/requests/{id}/choose     {"option": 2}   (or {"topic_id": "123"}, or {"force": true} for a duplicate)
  POST /api/requests/{id}/cancel
  GET  /api/wishlist                 what BookClub is watching the forum for
  POST /api/wishlist                 {"query": "Rina Kent", "kind": "author", "auto_download": false}
  DELETE /api/wishlist/{id}
  PUT  /api/integration/webhook      {"url": "..."} — the Home Assistant integration's own webhook
  DELETE /api/integration/webhook
"""
import secrets
from datetime import datetime, timedelta

from fastapi import APIRouter, Depends, HTTPException, Request
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field
from sqlalchemy import func
from sqlalchemy.orm import Session

from app.database import get_db
from app.models import BookRequest, Download, WishlistItem
from app.services import ai, book_requests, wishlist
from app.services.pipeline import pipeline, queue_positions
from app.settings_store import get_setting, set_setting

router = APIRouter()


def ensure_api_token(db: Session) -> str:
    """The token Home Assistant must send; created the first time it is needed."""
    token = get_setting(db, "api_token")
    if not token:
        token = secrets.token_urlsafe(24)
        set_setting(db, "api_token", token)
    return token


def require_token(request: Request, db: Session = Depends(get_db)):
    expected = ensure_api_token(db)
    auth = request.headers.get("authorization", "")
    given = auth[7:].strip() if auth.lower().startswith("bearer ") else request.headers.get("x-api-key", "")
    if not given or not secrets.compare_digest(given.encode(), expected.encode()):
        raise HTTPException(status_code=401, detail="Missing or wrong API token — see BookClub Settings → Home Assistant")


@router.get("/health")
def health(db: Session = Depends(get_db)):
    """503 when the database is unreachable or a background task has died (Docker marks it unhealthy)."""
    try:
        db.query(func.count(Download.id)).scalar()
    except Exception as exc:
        return JSONResponse({"status": "error", "database": str(exc)[:200]}, status_code=503)
    dead = [t.get_name() for t in pipeline.tasks if t.done()]
    body = {"status": "degraded" if dead else "ok", "pipeline": bool(pipeline.tasks) and not dead, "stopped": dead}
    return JSONResponse(body, status_code=503 if dead else 200)


@router.get("/status")
def status(db: Session = Depends(get_db)):
    """
    Summary for dashboards, e.g. a Home Assistant REST sensor:
      sensor: {platform: rest, resource: http://bookclub:9933/api/status,
               value_template: "{{ value_json.state }}", json_attributes: [...]}
    """
    def count(*criteria) -> int:
        return db.query(func.count(Download.id)).filter(*criteria).scalar() or 0

    since = datetime.utcnow() - timedelta(days=1)
    downloading = count(Download.status == "sent")
    queued = count(Download.m4b_status == "queued")
    converting = db.query(Download).filter(Download.m4b_status == "converting").first()
    failed = (count(Download.status == "failed")
              + count(Download.m4b_status.in_(("m4b_failed", "move_failed")))
              + count(Download.abs_status.in_(("not_found", "failed"))))
    needs_review = count(Download.status == "downloaded", Download.m4b_status.is_(None),
                         Download.auto_processed == True)  # noqa: E712 — SQL comparison
    positions = queue_positions(db)
    queued_titles = [db.get(Download, i).display_title for i in sorted(positions, key=positions.get)[:10]]
    imported_24h = count(Download.abs_status == "imported", Download.imported_at >= since)
    last = (db.query(Download).filter(Download.m4b_status == "converted")
            .order_by(Download.id.desc()).first())
    last_imported = (db.query(Download).filter(Download.imported_at.isnot(None))
                     .order_by(Download.imported_at.desc()).first())
    open_requests = db.query(func.count(BookRequest.id)).filter(
        BookRequest.status.in_(book_requests.ACTIVE_STATUSES)).scalar() or 0
    waiting_requests = db.query(func.count(BookRequest.id)).filter(BookRequest.status == "needs_choice").scalar() or 0

    if converting:
        state = "converting"
    elif downloading:
        state = "downloading"
    elif queued:
        state = "queued"
    elif open_requests:
        state = "searching"
    elif failed or needs_review or waiting_requests:
        state = "attention"
    else:
        state = "idle"

    plan = ai.plan_status(db)
    return {
        "state": state,
        "downloading": downloading,
        "queued": queued,
        "converting": converting.display_title if converting else None,
        "converting_progress": converting.m4b_progress if converting else None,
        "needs_review": needs_review,
        "failed": failed,
        "imported_last_24h": imported_24h,
        "last_converted": last.display_title if last else None,
        "last_imported": last_imported.display_title if last_imported else None,
        "last_imported_at": last_imported.imported_at.isoformat(timespec="seconds") + "Z" if last_imported else None,
        "queue": queued_titles,
        "requests_searching": open_requests,
        "requests_waiting": waiting_requests,
        # Only known while something is downloading (that's when NZBGet is asked); null otherwise.
        "nzbget_reachable": pipeline.nzbget_reachable,
        "last_poll": pipeline.last_poll_at.isoformat(timespec="seconds") + "Z" if pipeline.last_poll_at else None,
        "encoding_waiting": pipeline.encode_wait or None,
        "encoding_note": pipeline.encode_note or None,
        "wishlist_watching": db.query(func.count(WishlistItem.id)).filter(
            WishlistItem.status.in_(wishlist.ACTIVE)).scalar() or 0,
        "wishlist_found": db.query(func.count(WishlistItem.id)).filter(WishlistItem.status == "found").scalar() or 0,
        "ai_calls_today": ai.usage(db)["calls"],
        "ai_plan_5h_percent": (plan.get("five_hour") or {}).get("percent"),
    }


# ── requests ──────────────────────────────────────────────────────────────────

class NewRequest(BaseModel):
    query: str = Field(min_length=2, max_length=300)
    requested_by: str = ""
    force: bool = False


class Choice(BaseModel):
    option: int | None = None
    topic_id: str = ""
    force: bool = False


def _get_request(db: Session, request_id: int) -> BookRequest:
    req = db.get(BookRequest, request_id)
    if not req:
        raise HTTPException(status_code=404, detail="No such request")
    return req


@router.post("/requests", status_code=202, dependencies=[Depends(require_token)])
def create_request(body: NewRequest, db: Session = Depends(get_db)):
    query = body.query.strip()
    if not query:
        raise HTTPException(status_code=422, detail="Say which book")
    req = book_requests.create_request(db, query, body.requested_by, body.force)
    pipeline.run_soon(pipeline.process_request, req.id)
    return req.as_dict()


@router.get("/requests", dependencies=[Depends(require_token)])
def list_requests(limit: int = 10, db: Session = Depends(get_db)):
    rows = db.query(BookRequest).order_by(BookRequest.id.desc()).limit(max(1, min(limit, 100))).all()
    return [r.as_dict() for r in rows]


@router.get("/requests/{request_id}", dependencies=[Depends(require_token)])
def get_request(request_id: int, db: Session = Depends(get_db)):
    return _get_request(db, request_id).as_dict()


@router.post("/requests/{request_id}/choose", dependencies=[Depends(require_token)])
def choose(request_id: int, body: Choice, db: Session = Depends(get_db)):
    req = _get_request(db, request_id)
    error = book_requests.choose(db, req, body.option, body.topic_id, body.force)
    if error:
        raise HTTPException(status_code=409, detail=error)
    pipeline.run_soon(pipeline.process_request, req.id)
    return req.as_dict()


@router.post("/requests/{request_id}/cancel", dependencies=[Depends(require_token)])
def cancel(request_id: int, db: Session = Depends(get_db)):
    req = _get_request(db, request_id)
    if req.status in ("sent",):
        raise HTTPException(status_code=409, detail="Already sent to NZBGet — remove it on the Downloads page")
    req.status = "cancelled"
    req.updated_at = datetime.utcnow()
    db.commit()
    return req.as_dict()


# ── wishlist ──────────────────────────────────────────────────────────────────

class NewWish(BaseModel):
    query: str = Field(min_length=2, max_length=200)
    kind: str = "book"
    author: str = ""
    auto_download: bool = True
    requested_by: str = ""


@router.get("/wishlist", dependencies=[Depends(require_token)])
def list_wishlist(db: Session = Depends(get_db)):
    return [w.as_dict() for w in db.query(WishlistItem).order_by(WishlistItem.id.desc()).all()]


@router.post("/wishlist", status_code=201, dependencies=[Depends(require_token)])
def add_wish(body: NewWish, db: Session = Depends(get_db)):
    if body.kind not in wishlist.KINDS:
        raise HTTPException(status_code=422, detail=f"kind must be one of {', '.join(wishlist.KINDS)}")
    item, error = wishlist.add_item(db, body.kind, body.query, body.author, body.auto_download, body.requested_by)
    if error:
        raise HTTPException(status_code=409, detail=error)
    pipeline.run_soon(pipeline.check_wishlist_item, item.id)
    return item.as_dict()


@router.delete("/wishlist/{item_id}", dependencies=[Depends(require_token)])
def remove_wish(item_id: int, db: Session = Depends(get_db)):
    item = db.get(WishlistItem, item_id)
    if not item:
        raise HTTPException(status_code=404, detail="No such wishlist entry")
    db.delete(item)
    db.commit()
    return {"deleted": item_id}


# ── the Home Assistant integration ────────────────────────────────────────────

class Webhook(BaseModel):
    url: str = Field(min_length=8, max_length=500)


@router.put("/integration/webhook", dependencies=[Depends(require_token)])
def set_integration_webhook(body: Webhook, db: Session = Depends(get_db)):
    """The integration's own webhook: every event goes there too (your own webhook URL is left alone)."""
    if not body.url.startswith(("http://", "https://")):
        raise HTTPException(status_code=422, detail="The webhook URL must start with http:// or https://")
    set_setting(db, "ha_integration_webhook_url", body.url)
    return {"url": body.url}


@router.delete("/integration/webhook", dependencies=[Depends(require_token)])
def clear_integration_webhook(db: Session = Depends(get_db)):
    set_setting(db, "ha_integration_webhook_url", "")
    return {"url": ""}
