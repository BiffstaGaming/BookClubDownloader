"""GET /covers/{download_id} — a download's cover from BookClub's own cache."""
from fastapi import APIRouter, Depends
from fastapi.responses import FileResponse, RedirectResponse, Response
from sqlalchemy.orm import Session

from app.database import get_db
from app.models import Download
from app.services import covers

router = APIRouter()


@router.get("/{download_id}")
def cover(download_id: int, db: Session = Depends(get_db)):
    """Plain `def`: the first request for a cover downloads it."""
    dl = db.get(Download, download_id)
    url = (dl.parsed_metadata.get("cover_url") if dl else "") or ""
    if not url:
        return Response(status_code=404)
    path = covers.cached_path(url)
    if not path:
        return RedirectResponse(url, status_code=302)  # let the browser try the original
    # The URL carries ?v=<hash of the cover URL>, so a changed cover is a new URL.
    return FileResponse(path, media_type=covers.media_type(path),
                        headers={"Cache-Control": "public, max-age=604800"})
