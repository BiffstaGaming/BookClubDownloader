"""
GET /events — Server-Sent Events: tells open pages what changed (see app/events.py).

    event: change
    data: downloads,requests

The browser shares one stream between all its BookClub tabs (static/js/bookclub.js)
and falls back to polling when the stream isn't available.
"""
import asyncio

from fastapi import APIRouter, Request
from fastapi.responses import StreamingResponse

from app.events import bus

router = APIRouter()

PING_SECONDS = 20         # keeps proxies from closing an idle stream
MAX_SECONDS = 300         # the browser reconnects by itself; also bounds a stream at shutdown
COALESCE_SECONDS = 0.4    # a burst of commits becomes one message


async def event_stream(request: Request, ping_seconds: float = None, max_seconds: float = None):
    ping_seconds = PING_SECONDS if ping_seconds is None else ping_seconds
    max_seconds = MAX_SECONDS if max_seconds is None else max_seconds
    queue = bus.subscribe()
    loop = asyncio.get_running_loop()
    ends_at = loop.time() + max_seconds
    try:
        yield "retry: 3000\n\n"
        yield "event: hello\ndata: ok\n\n"
        while not bus.closed and loop.time() < ends_at:
            try:
                topics = await asyncio.wait_for(queue.get(), timeout=min(ping_seconds, max(0.01, ends_at - loop.time())))
            except asyncio.TimeoutError:
                if await request.is_disconnected():
                    return
                yield ": ping\n\n"
                continue
            if topics is None:
                return
            await asyncio.sleep(COALESCE_SECONDS)
            while not queue.empty():
                more = queue.get_nowait()
                if more is None:
                    return
                topics |= more
            yield f"event: change\ndata: {','.join(sorted(topics))}\n\n"
    finally:
        bus.unsubscribe(queue)


@router.get("/events")
async def events(request: Request):
    return StreamingResponse(
        event_stream(request),
        media_type="text/event-stream",
        headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
    )
