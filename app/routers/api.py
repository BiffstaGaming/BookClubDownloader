"""JSON endpoints for Home Assistant (REST sensors) and container health checks."""
from datetime import datetime, timedelta

from fastapi import APIRouter, Depends
from fastapi.responses import JSONResponse
from sqlalchemy import func
from sqlalchemy.orm import Session

from app.database import get_db
from app.models import Download
from app.services.pipeline import pipeline, queue_positions

router = APIRouter()


@router.get("/health")
def health(db: Session = Depends(get_db)):
    """503 when the database is unreachable or a background task has died (Docker marks it unhealthy)."""
    db.query(func.count(Download.id)).scalar()
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
    needs_review = sum(
        1 for dl in db.query(Download).filter(Download.status == "downloaded", Download.m4b_status.is_(None)).all()
        if dl.parsed_metadata.get("auto_processed")
    )
    positions = queue_positions(db)
    queued_titles = [db.get(Download, i).display_title for i in sorted(positions, key=positions.get)][:10]
    imported_24h = count(Download.abs_status == "imported", Download.created_at >= since)
    last = (db.query(Download).filter(Download.m4b_status == "converted")
            .order_by(Download.id.desc()).first())

    if converting:
        state = "converting"
    elif downloading:
        state = "downloading"
    elif queued:
        state = "queued"
    elif failed or needs_review:
        state = "attention"
    else:
        state = "idle"

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
        "queue": queued_titles,
        "nzbget_reachable": not pipeline.last_poll_error,
        "last_poll": pipeline.last_poll_at.isoformat(timespec="seconds") + "Z" if pipeline.last_poll_at else None,
    }
