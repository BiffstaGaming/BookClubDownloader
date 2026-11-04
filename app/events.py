"""
Live updates for open pages. Every database commit that touches downloads,
requests, logs, the wishlist or saved posts announces it here; GET /events streams
those announcements to the browser (Server-Sent Events), and the pages refresh
just the part that changed instead of asking every few seconds.

The announcement is only "what kind of thing changed" — never the data itself.
"""
import asyncio
import logging
import threading

from sqlalchemy import event
from sqlalchemy.orm import Session

logger = logging.getLogger(__name__)

# Model class name → topic the pages listen for
TOPICS = {
    "Download": "downloads",
    "BookRequest": "requests",
    "LogEntry": "logs",
    "WishlistItem": "wishlist",
    "RevealedPost": "saved",
}
ALL_TOPICS = tuple(sorted(set(TOPICS.values())))


class EventBus:
    def __init__(self):
        self.loop: asyncio.AbstractEventLoop | None = None
        self.subscribers: set[asyncio.Queue] = set()
        self.closed = False
        self._lock = threading.Lock()

    def attach(self, loop: asyncio.AbstractEventLoop):
        self.loop = loop
        self.closed = False

    def close(self):
        """Server shutting down: end every open stream."""
        self.closed = True
        self._deliver_threadsafe(None)

    def subscribe(self) -> asyncio.Queue:
        queue: asyncio.Queue = asyncio.Queue()
        with self._lock:
            self.subscribers.add(queue)
        return queue

    def unsubscribe(self, queue: asyncio.Queue):
        with self._lock:
            self.subscribers.discard(queue)

    def publish(self, topics):
        """Announce changed topics. Safe from any thread; does nothing until the server runs."""
        topics = {t for t in topics if t}
        if topics and self.subscribers:
            self._deliver_threadsafe(topics)

    def _deliver_threadsafe(self, item):
        loop = self.loop
        if loop is None or loop.is_closed():
            return
        try:
            loop.call_soon_threadsafe(self._deliver, item)
        except RuntimeError:  # loop shut down meanwhile
            pass

    def _deliver(self, item):
        with self._lock:
            queues = list(self.subscribers)
        for queue in queues:
            queue.put_nowait(set(item) if item is not None else None)


bus = EventBus()


# ── which topics a commit touched ─────────────────────────────────────────────

def _remember(session: Session, topic: str | None):
    if topic:
        session.info.setdefault("bc_topics", set()).add(topic)


@event.listens_for(Session, "after_flush")
def _collect_flushed(session, _flush_context):
    # Still the pre-flush lists here, which is what was written.
    for obj in (*session.new, *session.dirty, *session.deleted):
        _remember(session, TOPICS.get(type(obj).__name__))


@event.listens_for(Session, "do_orm_execute")
def _collect_bulk(orm_execute_state):
    # query(...).delete() / .update() skip the flush lists.
    if orm_execute_state.is_delete or orm_execute_state.is_update:
        mapper = orm_execute_state.bind_mapper
        if mapper is not None:
            _remember(orm_execute_state.session, TOPICS.get(mapper.class_.__name__))


@event.listens_for(Session, "after_commit")
def _announce(session):
    topics = session.info.pop("bc_topics", None)
    if topics:
        bus.publish(topics)


@event.listens_for(Session, "after_rollback")
def _forget(session):
    session.info.pop("bc_topics", None)
