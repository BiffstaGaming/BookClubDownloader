"""
The unattended part of BookClub. Runs inside the web process as asyncio tasks:

  poller  — every few seconds asks NZBGet about downloads still in progress,
            records completions/failures, and auto-matches finished books.
  worker  — converts one book at a time. The queue lives in the database
            (m4b_status="queued"), so it survives restarts and container updates.
  import  — after conversion, gets Audiobookshelf to pick up the new folder
            and Quick Match it.

Nothing here depends on a browser being open.
"""
import asyncio
import logging
import os
import re
import shutil
import signal
from datetime import datetime, timedelta

from sqlalchemy import or_

from app.database import SessionLocal
from app.log_handler import log_to_db
from app.models import Download, LogEntry
from app.services import library, notify
from app.services.abs import AbsClient
from app.services.matching import normalize, parse_nzb_name, rank_candidates
from app.services.nzbget import NzbgetClient
from app.settings_store import get_bool, get_int, get_setting

logger = logging.getLogger(__name__)

MISSING_POLLS_BEFORE_FAILURE = 6

# m4b-tool reads these from the input folder; anything else (nfo, jpg, txt) is ignored.
MERGE_EXTENSIONS = "mp3,m4a,m4b,mp4,aac,flac,ogg,oga,opus,wav,wma"

_HISTORY_MESSAGES = {
    "WARNING/PASSWORD": "Archive password was wrong or missing — check the password from the forum post",
    "FAILURE/PASSWORD": "Archive password was wrong or missing — check the password from the forum post",
    "WARNING/DAMAGED": "Download is damaged and could not be repaired — try another NZB",
    "WARNING/REPAIRABLE": "Download needs repair but par-check was skipped — try another NZB",
    "WARNING/SPACE": "NZBGet ran out of disk space",
    "WARNING/HEALTH": "Too many missing articles (expired on the server) — try another NZB",
    "FAILURE/HEALTH": "Too many missing articles (expired on the server) — try another NZB",
    "FAILURE/PAR": "Par repair failed — try another NZB",
    "FAILURE/UNPACK": "Unpacking failed — usually a wrong password or a corrupt archive",
    "FAILURE/MOVE": "NZBGet could not move the files to the destination folder",
    "FAILURE/BAD": "The NZB file was malformed",
    "FAILURE/SCAN": "NZBGet could not read the NZB file",
    "FAILURE/FETCH": "NZBGet could not fetch the NZB",
    "DELETED/MANUAL": "Deleted in NZBGet",
    "DELETED/DUPE": "NZBGet skipped it as a duplicate of an earlier download",
    "DELETED/COPY": "NZBGet skipped it as an exact copy of an earlier download",
    "DELETED/GOOD": "NZBGet removed it because a duplicate already succeeded",
    "DELETED/HEALTH": "NZBGet's health check deleted it (too many missing articles) — try another NZB",
    "DELETED/SCAN": "NZBGet could not read the NZB file",
}


def classify_history_status(status: str) -> tuple[str, str]:
    """
    Map an NZBGet history Status ("SUCCESS/ALL", "WARNING/PASSWORD", …) to
    ("downloaded" | "failed" | "", message).
    """
    s = (status or "").upper()
    kind, _, detail = s.partition("/")
    if kind == "SUCCESS":
        return "downloaded", ""
    if s == "WARNING/SCRIPT":
        return "downloaded", "NZBGet post-processing script reported a warning"
    if kind in ("WARNING", "FAILURE", "DELETED"):
        return "failed", _HISTORY_MESSAGES.get(s, f"NZBGet finished with status {s}")
    return "", ""


def queue_positions(db) -> dict:
    """{download_id: position} for queued conversions, in the order the worker will take them."""
    queued = (
        db.query(Download)
        .filter(Download.m4b_status == "queued")
        .order_by(Download.queued_at.is_(None), Download.queued_at, Download.id)
        .all()
    )
    return {dl.id: i + 1 for i, dl in enumerate(queued)}


def download_roots(db) -> list[str]:
    """Folders that finished downloads live under — the only places we delete from."""
    roots = [get_setting(db, "local_path_prefix"), get_setting(db, "nzbget_path_prefix"), "/downloads"]
    return [r for r in dict.fromkeys(roots) if r]


def abs_configured(db) -> bool:
    return bool(get_setting(db, "abs_url") and get_setting(db, "abs_token") and get_setting(db, "abs_library_id"))


class Pipeline:
    def __init__(self):
        self.loop: asyncio.AbstractEventLoop | None = None
        self.wake = asyncio.Event()
        self.tasks: list[asyncio.Task] = []
        self.background: set[asyncio.Task] = set()
        self.current_id: int | None = None
        self.current_proc: asyncio.subprocess.Process | None = None
        self.cancelled: set[int] = set()
        self.last_poll_error = ""
        self.last_prune: datetime | None = None
        self.last_poll_at: datetime | None = None

    # ── lifecycle ─────────────────────────────────────────────────────────────
    async def start(self):
        self.loop = asyncio.get_running_loop()
        self.wake = asyncio.Event()
        self.recover()
        self.tasks = [
            asyncio.create_task(self._poll_loop(), name="bookclub-poller"),
            asyncio.create_task(self._worker_loop(), name="bookclub-worker"),
        ]
        db = SessionLocal()
        try:
            pending_imports = [d.id for d in db.query(Download).filter(Download.abs_status == "pending").all()]
        finally:
            db.close()
        for dl_id in pending_imports:
            self.spawn(self.abs_import(dl_id))
        logger.info("Pipeline started (poller + conversion worker)")

    async def stop(self):
        self._kill_current()
        for task in [*self.tasks, *self.background]:
            task.cancel()
        await asyncio.gather(*self.tasks, *self.background, return_exceptions=True)
        self.tasks.clear()
        self.background.clear()

    def spawn(self, coro):
        task = asyncio.create_task(coro)
        self.background.add(task)
        task.add_done_callback(self.background.discard)
        return task

    def queue_changed(self):
        """Wake the worker. Safe to call from request threads."""
        if self.loop and not self.loop.is_closed():
            self.loop.call_soon_threadsafe(self.wake.set)

    def run_soon(self, coro_fn, *args):
        """Schedule a pipeline coroutine from a request thread."""
        if self.loop and not self.loop.is_closed():
            self.loop.call_soon_threadsafe(lambda: self.spawn(coro_fn(*args)))

    def recover(self):
        """A restart kills any running m4b-tool — put those books back in the queue."""
        db = SessionLocal()
        try:
            stuck = db.query(Download).filter(Download.m4b_status == "converting").all()
            for dl in stuck:
                dl.m4b_status = "queued"
                dl.m4b_progress = None
                dl.queued_at = dl.queued_at or datetime.utcnow()
                log_to_db("WARNING", "conversion",
                          f"BookClub restarted during conversion of #{dl.id} — re-queued", download_id=dl.id)
            db.commit()
        finally:
            db.close()

    # ── NZBGet poller ─────────────────────────────────────────────────────────
    async def _poll_loop(self):
        while True:
            try:
                await self.poll_once()
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                logger.exception("Poller error: %s", exc)
            db = SessionLocal()
            try:
                interval = max(5, get_int(db, "poll_interval", 15))
            finally:
                db.close()
            await asyncio.sleep(interval)

    async def poll_once(self):
        self.last_poll_at = datetime.utcnow()
        completed, failed, to_match = [], [], []
        db = SessionLocal()
        try:
            nzbget_url = get_setting(db, "nzbget_url")
            pending = db.query(Download).filter(
                Download.status == "sent", Download.nzbget_id.isnot(None)
            ).all()

            if pending and nzbget_url:
                client = NzbgetClient(nzbget_url, get_setting(db, "nzbget_username"), get_setting(db, "nzbget_password"))
                try:
                    history, queue = await asyncio.to_thread(lambda: (client.get_history(), client.get_queue()))
                except Exception as exc:
                    message = f"Cannot reach NZBGet at {nzbget_url}: {exc}"
                    if message != self.last_poll_error:
                        log_to_db("WARNING", "poller", message)
                    self.last_poll_error = message
                    history = queue = None
                if history is not None:
                    if self.last_poll_error:
                        log_to_db("INFO", "poller", "NZBGet reachable again")
                    self.last_poll_error = ""
                    self._apply_nzbget_state(db, pending, history, queue, completed, failed)
                    db.commit()

            # Restart-safe: anything finished but never auto-matched gets matched now.
            for dl in db.query(Download).filter(Download.status == "downloaded", Download.m4b_status.is_(None)).all():
                if not dl.parsed_metadata.get("auto_processed"):
                    to_match.append(dl.id)
        finally:
            db.close()

        for dl_id, title in completed:
            await notify.send_event("download_completed", title=title, download_id=dl_id)
        for dl_id, title, message in failed:
            await notify.send_event("download_failed", title=title, download_id=dl_id, message=message)
        for dl_id in to_match:
            await self.auto_process(dl_id)

        await self._maybe_prune_logs()

    def _apply_nzbget_state(self, db, pending, history, queue, completed, failed):
        history_by_id = {item.get("NZBID"): item for item in history}
        queue_by_id = {item.get("NZBID"): item for item in queue}
        nzbget_prefix = get_setting(db, "nzbget_path_prefix")
        local_prefix = get_setting(db, "local_path_prefix")

        for dl in pending:
            q_item = queue_by_id.get(dl.nzbget_id)
            h_item = history_by_id.get(dl.nzbget_id)
            if q_item:
                dl.missing_polls = 0
                file_mb = q_item.get("FileSizeMB", 0)
                remaining_mb = q_item.get("RemainingSizeMB", 0)
                if file_mb > 0:
                    dl.download_progress = max(0, min(99, round((file_mb - remaining_mb) / file_mb * 100)))
                continue
            if not h_item:
                dl.missing_polls = (dl.missing_polls or 0) + 1
                if dl.missing_polls >= MISSING_POLLS_BEFORE_FAILURE:
                    dl.status = "failed"
                    dl.error_message = "No longer in NZBGet's queue or history (deleted or history cleared)"
                    log_to_db("ERROR", "poller", f"#{dl.id}: {dl.error_message}", download_id=dl.id)
                    failed.append((dl.id, dl.display_title, dl.error_message))
                continue

            outcome, message = classify_history_status(h_item.get("Status", ""))
            if outcome == "downloaded":
                dl.status = "downloaded"
                dl.download_progress = 100
                dl.error_message = message or None
                final_dir = h_item.get("FinalDir") or h_item.get("DestDir", "")
                if final_dir:
                    dl.download_path = library.map_path(final_dir, nzbget_prefix, local_prefix)
                log_to_db("INFO", "poller", f"#{dl.id} downloaded to {dl.download_path}", download_id=dl.id)
                completed.append((dl.id, dl.display_title))
            elif outcome == "failed":
                dl.status = "failed"
                dl.error_message = message
                log_to_db("ERROR", "poller", f"#{dl.id} failed in NZBGet: {message}", download_id=dl.id)
                failed.append((dl.id, dl.display_title, message))

    async def _maybe_prune_logs(self):
        now = datetime.utcnow()
        if self.last_prune and now - self.last_prune < timedelta(hours=1):
            return
        self.last_prune = now
        db = SessionLocal()
        try:
            days = get_int(db, "log_retention_days", 14)
            if days > 0:
                cutoff = now - timedelta(days=days)
                deleted = db.query(LogEntry).filter(LogEntry.created_at < cutoff).delete()
                db.commit()
                if deleted:
                    logger.info("Pruned %d log entries older than %d days", deleted, days)
        finally:
            db.close()

    # ── auto-match ────────────────────────────────────────────────────────────
    async def auto_process(self, download_id: int):
        """
        Match a finished download against Audible (through ABS) and queue the
        conversion when the match is confident enough.
        """
        db = SessionLocal()
        try:
            dl = db.query(Download).filter(Download.id == download_id).first()
            if not dl or dl.status != "downloaded":
                return
            meta = dl.parsed_metadata
            if meta.get("auto_processed"):
                return
            meta["auto_processed"] = True
            dl.set_metadata(meta)
            db.commit()

            title_hint = dl.display_title
            if not (get_setting(db, "abs_url") and get_setting(db, "abs_token")):
                log_to_db("INFO", "auto", f"#{download_id}: Audiobookshelf not configured — convert manually", download_id=download_id)
                await notify.send_event("needs_review", title=title_hint, download_id=download_id,
                                        message="Audiobookshelf is not configured, so the book could not be matched automatically")
                return

            nzb_name = dl.post_title or dl.nzb_name or ""
            nzb_title, nzb_author = parse_nzb_name(nzb_name)
            forum_author = meta.get("author", "")
            if forum_author and re.search(r'\d$', forum_author.strip()):
                forum_author = ""  # looks like "Series Name 1", not a real author name
            query_title = nzb_title or meta.get("title", "")
            query_author = nzb_author or forum_author
            provider = get_setting(db, "audible_provider")
            if not query_title:
                log_to_db("WARNING", "auto", f"#{download_id}: could not extract a title from '{nzb_name}'", download_id=download_id)
                await notify.send_event("needs_review", title=title_hint, download_id=download_id,
                                        message="Could not work out the book title")
                return

            log_to_db("INFO", "auto", f"#{download_id}: searching {provider} — title='{query_title}' author='{query_author}'",
                      download_id=download_id)
            client = AbsClient(get_setting(db, "abs_url"), get_setting(db, "abs_token"))
            try:
                results = await asyncio.to_thread(client.search_books, query_title.strip(), query_author.strip(), provider)
            except Exception as exc:
                log_to_db("ERROR", "auto", f"#{download_id}: Audible search failed: {exc}", download_id=download_id)
                await notify.send_event("needs_review", title=title_hint, download_id=download_id,
                                        message=f"Audible search failed: {exc}")
                return

            candidates = rank_candidates(results, query_title, query_author)
            if not candidates:
                meta["match_confidence"] = 0
                dl.set_metadata(meta)
                db.commit()
                log_to_db("WARNING", "auto", f"#{download_id}: no Audible results", download_id=download_id)
                await notify.send_event("needs_review", title=title_hint, download_id=download_id,
                                        message="No Audible results — pick the details manually")
                return

            best = candidates[0]
            apply_candidate(meta, best)
            dl.set_metadata(meta)
            db.commit()
            confidence = best["confidence"]
            log_to_db("INFO", "auto",
                      f"#{download_id}: best match '{best['title']}' by '{best['author']}' — {confidence}%",
                      download_id=download_id)

            threshold = get_int(db, "auto_match_threshold", 90)
            if not get_bool(db, "auto_convert"):
                await notify.send_event("needs_review", title=meta.get("title"), author=meta.get("author"),
                                        download_id=download_id, message="Auto-convert is off — review and convert")
                return
            if confidence < threshold:
                log_to_db("INFO", "auto", f"#{download_id}: {confidence}% is below {threshold}% — manual review needed",
                          download_id=download_id)
                await notify.send_event("needs_review", title=meta.get("title"), author=meta.get("author"),
                                        download_id=download_id,
                                        message=f"Best Audible match is only {confidence}% — pick the right one")
                return

            problem = check_input_folder(dl.download_path)
            if problem:
                log_to_db("WARNING", "auto", f"#{download_id}: {problem} — not converting", download_id=download_id)
                await notify.send_event("needs_review", title=meta.get("title"), download_id=download_id, message=problem)
                return

            meta["input_path"] = dl.download_path
            dl.set_metadata(meta)
            enqueue(dl)
            db.commit()
            log_to_db("INFO", "auto", f"#{download_id}: queued for conversion ({confidence}% ≥ {threshold}%)",
                      download_id=download_id)
            self.queue_changed()
        finally:
            db.close()

    # ── conversion worker ─────────────────────────────────────────────────────
    async def _worker_loop(self):
        while True:
            dl_id = self._next_queued()
            if dl_id is None:
                self.wake.clear()
                try:
                    await asyncio.wait_for(self.wake.wait(), timeout=30)
                except asyncio.TimeoutError:
                    pass
                continue
            try:
                await self.convert(dl_id)
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                logger.exception("Conversion crashed for #%s", dl_id)
                await self._fail(dl_id, "m4b_failed", f"Unexpected error: {exc}")

    def _next_queued(self) -> int | None:
        db = SessionLocal()
        try:
            positions = queue_positions(db)
            return min(positions, key=positions.get) if positions else None
        finally:
            db.close()

    async def _fail(self, dl_id: int, status: str, message: str, log: str = None, m4b_path: str = None):
        db = SessionLocal()
        try:
            dl = db.query(Download).filter(Download.id == dl_id).first()
            if dl:
                dl.m4b_status = status
                dl.m4b_progress = None
                dl.error_message = message
                if log is not None:
                    dl.conversion_log = log[-4000:]
                if m4b_path:
                    dl.m4b_path = m4b_path
                db.commit()
                title = dl.display_title
            else:
                title = ""
        finally:
            db.close()
        log_to_db("ERROR", "conversion", f"#{dl_id}: {message}", download_id=dl_id)
        await notify.send_event("conversion_failed", title=title, download_id=dl_id, message=message)

    def cancel(self, dl_id: int) -> bool:
        """Stop a running conversion. Returns False if it isn't the one running."""
        if self.current_id != dl_id or not self.current_proc:
            return False
        self.cancelled.add(dl_id)
        self._kill_current()
        return True

    def _kill_current(self):
        proc = self.current_proc
        if not proc or proc.returncode is not None:
            return
        try:
            if hasattr(os, "killpg"):
                os.killpg(proc.pid, signal.SIGKILL)  # m4b-tool spawns ffmpeg children
            else:
                proc.kill()
        except ProcessLookupError:
            pass

    async def convert(self, dl_id: int):
        db = SessionLocal()
        try:
            dl = db.query(Download).filter(Download.id == dl_id).first()
            if not dl or dl.m4b_status != "queued":
                return
            meta = dl.parsed_metadata
            input_path = meta.get("input_path") or dl.download_path or ""
            output_root = get_setting(db, "m4b_output_path")
            move_template = get_setting(db, "m4b_move_template")
            jobs = get_setting(db, "m4b_jobs")
            bitrate = get_setting(db, "m4b_bitrate")
            delete_source = get_bool(db, "delete_source")
            roots = download_roots(db)
            dl.m4b_status = "converting"
            dl.m4b_progress = None
            dl.error_message = None
            dl.abs_status = None
            dl.abs_item_id = None
            db.commit()
        finally:
            db.close()

        self.current_id = dl_id
        work_dir = os.path.join(output_root, ".bookclub-work", str(dl_id))
        keep_work_dir = False
        try:
            problem = check_input_folder(input_path)
            if problem:
                await self._fail(dl_id, "m4b_failed", problem)
                return

            final_path = library.final_book_path(output_root, move_template, meta)
            if os.path.exists(final_path):
                await self._fail(dl_id, "m4b_failed",
                           f"A book already exists at {final_path} — delete it or change the title/series first")
                return

            shutil.rmtree(work_dir, ignore_errors=True)
            os.makedirs(work_dir, exist_ok=True)
            work_file = os.path.join(work_dir, os.path.basename(final_path))

            cover = library.find_cover_image(input_path)
            if not cover and meta.get("cover_url"):
                downloaded = os.path.join(work_dir, "cover.jpg")
                if await asyncio.to_thread(library.download_cover, meta["cover_url"], downloaded):
                    cover = downloaded

            audio = library.find_audio_files(input_path)
            log_to_db("INFO", "conversion", f"#{dl_id}: converting {len(audio)} audio file(s) from {input_path}",
                      download_id=dl_id)
            if len(audio) == 1 and audio[0].lower().endswith((".m4b", ".m4a", ".mp4")):
                # Already a single AAC file — copy it (the source stays intact until the end) and tag it.
                await asyncio.to_thread(shutil.copy2, audio[0], work_file)
                returncode, log = await self._run(dl_id, tone_command(work_file, meta, cover))
            else:
                returncode, log = await self._run(
                    dl_id, merge_command(input_path, work_file, meta, cover, jobs, bitrate))
            log_to_db("DEBUG", "conversion", f"#{dl_id} tool output:\n{log[-20000:]}", download_id=dl_id)

            if dl_id in self.cancelled:
                self.cancelled.discard(dl_id)
                db = SessionLocal()
                try:
                    dl = db.query(Download).filter(Download.id == dl_id).first()
                    if dl:
                        dl.m4b_status = None
                        dl.m4b_progress = None
                        dl.error_message = "Conversion cancelled"
                        db.commit()
                finally:
                    db.close()
                log_to_db("INFO", "conversion", f"#{dl_id}: conversion cancelled", download_id=dl_id)
                return

            if returncode != 0 or not os.path.isfile(work_file) or os.path.getsize(work_file) == 0:
                reason = f"m4b-tool exited with code {returncode}" if returncode else "no output file was produced"
                if "no files to convert" in log.lower():
                    reason = "m4b-tool found no audio files to convert"
                await self._fail(dl_id, "m4b_failed", f"Conversion failed: {reason} — see the log", log=log)
                return

            # Move into the library. Never overwrite an existing book.
            book_dir = os.path.dirname(final_path)
            try:
                os.makedirs(book_dir, exist_ok=True)
                if os.path.exists(final_path):
                    raise FileExistsError(f"{final_path} appeared while converting")
                await asyncio.to_thread(shutil.move, work_file, final_path)
            except Exception as exc:
                keep_work_dir = True
                await self._fail(dl_id, "move_failed", f"Could not move the book into the library: {exc} "
                           f"(the converted file is kept at {work_file})", log=log, m4b_path=work_file)
                return

            try:
                library.write_abs_metadata(book_dir, meta)
                if cover and not os.path.exists(os.path.join(book_dir, "cover.jpg")):
                    shutil.copyfile(cover, os.path.join(book_dir, "cover" + (os.path.splitext(cover)[1].lower() or ".jpg")))
            except Exception as exc:
                log_to_db("WARNING", "conversion", f"#{dl_id}: could not write metadata.json/cover: {exc}", download_id=dl_id)

            if delete_source:
                try:
                    deleted, reason = library.safe_delete_source(
                        input_path, roots, protected=[output_root, book_dir])
                    log_to_db("INFO" if deleted else "WARNING", "conversion",
                              f"#{dl_id}: source folder {input_path}: {reason}", download_id=dl_id)
                except Exception as exc:
                    log_to_db("WARNING", "conversion", f"#{dl_id}: could not delete {input_path}: {exc}", download_id=dl_id)

            db = SessionLocal()
            try:
                dl = db.query(Download).filter(Download.id == dl_id).first()
                dl.m4b_status = "converted"
                dl.m4b_progress = 100
                dl.m4b_path = final_path
                dl.conversion_log = log[-4000:]
                dl.abs_status = "pending" if abs_configured(db) else "skipped"
                run_import = dl.abs_status == "pending"
                db.commit()
            finally:
                db.close()
            log_to_db("INFO", "conversion", f"#{dl_id}: M4B ready at {final_path}", download_id=dl_id)
            await notify.send_event("book_converted", title=meta.get("title"), author=meta.get("author"),
                                    download_id=dl_id, message=final_path)
            if run_import:
                self.spawn(self.abs_import(dl_id))
        finally:
            self.current_id = None
            self.current_proc = None
            if not keep_work_dir:
                shutil.rmtree(work_dir, ignore_errors=True)

    async def _run(self, dl_id: int, cmd: list[str]) -> tuple[int, str]:
        """Run a tool, streaming output so progress percentages reach the UI."""
        logger.info("Running for #%d: %s", dl_id, " ".join(cmd))
        kwargs = {"start_new_session": True} if os.name != "nt" else {}
        try:
            proc = await asyncio.create_subprocess_exec(
                *cmd, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.STDOUT, **kwargs)
        except FileNotFoundError as exc:
            return 127, f"{cmd[0]} is not installed: {exc}"
        self.current_proc = proc
        buf, lines, last_progress = "", [], -1
        while True:
            chunk = await proc.stdout.read(1024)
            if not chunk:
                break
            buf += chunk.decode("utf-8", errors="replace")
            parts = re.split(r'\r\n|\n|\r', buf)
            buf = parts[-1]
            for line in [*parts[:-1], buf]:
                m = re.search(r'\b(\d{1,3})%', line)
                if m:
                    pct = min(99, int(m.group(1)))
                    if pct != last_progress:
                        last_progress = pct
                        self._set_progress(dl_id, pct)
            lines.extend(line for line in parts[:-1] if line.strip())
        if buf.strip():
            lines.append(buf)
        await proc.wait()
        return proc.returncode, "\n".join(lines)

    def _set_progress(self, dl_id: int, pct: int):
        db = SessionLocal()
        try:
            dl = db.query(Download).filter(Download.id == dl_id).first()
            if dl:
                dl.m4b_progress = pct
                db.commit()
        except Exception:
            pass
        finally:
            db.close()

    # ── Audiobookshelf import ────────────────────────────────────────────────
    async def abs_import(self, dl_id: int, poll_seconds: int = 10, attempts: int = 24):
        db = SessionLocal()
        try:
            dl = db.query(Download).filter(Download.id == dl_id).first()
            if not dl or not dl.m4b_path:
                return
            if not abs_configured(db):
                dl.abs_status = "skipped"
                db.commit()
                return
            meta = dl.parsed_metadata
            m4b_path = dl.m4b_path
            abs_url, token = get_setting(db, "abs_url"), get_setting(db, "abs_token")
            library_id = get_setting(db, "abs_library_id")
            provider = get_setting(db, "audible_provider")
            abs_file = library.map_path(m4b_path, get_setting(db, "abs_path_local"), get_setting(db, "abs_path_remote"))
            dl.abs_status = "pending"
            db.commit()
        finally:
            db.close()

        client = AbsClient(abs_url, token)
        abs_dir = os.path.dirname(abs_file).replace("\\", "/").rstrip("/")
        local_dir = os.path.dirname(m4b_path).replace("\\", "/").rstrip("/")
        title = meta.get("title") or ""
        try:
            await asyncio.to_thread(client.notify_new_path, library_id, abs_file)
            log_to_db("INFO", "abs", f"#{dl_id}: asked Audiobookshelf to scan {abs_file}", download_id=dl_id)
        except Exception as exc:
            log_to_db("DEBUG", "abs", f"#{dl_id}: watcher update not accepted ({exc}) — will run a library scan",
                      download_id=dl_id)

        item, scanned = None, False
        for attempt in range(attempts):
            await asyncio.sleep(poll_seconds)
            if attempt == 3 and not scanned:
                # The watcher may be disabled for this library; fall back to a full scan once.
                try:
                    await asyncio.to_thread(client.scan_library, library_id)
                    log_to_db("INFO", "abs", f"#{dl_id}: library scan triggered", download_id=dl_id)
                except Exception as exc:
                    log_to_db("WARNING", "abs", f"#{dl_id}: library scan failed: {exc}", download_id=dl_id)
                scanned = True
            try:
                item = await asyncio.to_thread(find_library_item, client, library_id, title, meta.get("asin", ""),
                                               abs_dir, local_dir)
            except Exception as exc:
                log_to_db("DEBUG", "abs", f"#{dl_id}: ABS search attempt {attempt + 1} failed: {exc}", download_id=dl_id)
            if item:
                break

        db = SessionLocal()
        try:
            dl = db.query(Download).filter(Download.id == dl_id).first()
            if not dl:
                return
            if not item:
                dl.abs_status = "not_found"
                dl.error_message = (f"Audiobookshelf has not picked up {abs_dir} yet. Check that this folder is inside "
                                    f"the ABS library and the BookClub → ABS path mapping in Settings.")
                db.commit()
                log_to_db("WARNING", "abs", f"#{dl_id}: {dl.error_message}", download_id=dl_id)
                await notify.send_event("abs_import_failed", title=title, download_id=dl_id, message=dl.error_message)
                return
            dl.abs_item_id = item["id"]
            dl.abs_status = "imported"
            db.commit()
        finally:
            db.close()

        log_to_db("INFO", "abs", f"#{dl_id}: found in Audiobookshelf (item {item['id']})", download_id=dl_id)
        try:
            result = await asyncio.to_thread(client.quick_match, item["id"], title, meta.get("author", ""),
                                             provider, meta.get("asin", ""))
            if result.get("warning"):
                log_to_db("WARNING", "abs", f"#{dl_id}: Quick Match: {result['warning']}", download_id=dl_id)
            else:
                log_to_db("INFO", "abs", f"#{dl_id}: Quick Match done (updated={result.get('updated')})", download_id=dl_id)
        except Exception as exc:
            log_to_db("WARNING", "abs", f"#{dl_id}: Quick Match failed: {exc}", download_id=dl_id)
        await notify.send_event("book_imported", title=title, author=meta.get("author"), download_id=dl_id,
                                url=client.item_web_url(item["id"]))


def _path_tail(path: str, depth: int) -> str:
    parts = [p for p in re.split(r"[\\/]+", path or "") if p]
    return "/".join(parts[-depth:]).lower()


def find_library_item(client: AbsClient, library_id: str, title: str, asin: str,
                      abs_dir: str, local_dir: str) -> dict | None:
    """
    Find the ABS item for a folder we just wrote. Matches on the folder path
    (exact ABS path, else the last folders of it), never on title alone, so an
    older copy of the same book is not mistaken for the new one.
    """
    seen = {}
    for query in [q for q in (title, asin) if q]:
        for item in client.search_library(library_id, query):
            seen[item.get("id")] = item
    depth = min(2, len([p for p in re.split(r"[\\/]+", local_dir) if p]))
    for item in seen.values():
        path = (item.get("path") or "").replace("\\", "/").rstrip("/")
        if path == abs_dir:
            return item
    for item in seen.values():
        if depth and _path_tail(item.get("path"), depth) == _path_tail(local_dir, depth):
            return item
    return None


def check_input_folder(path: str) -> str:
    """Return a problem description, or "" if the folder holds audio we can convert."""
    if not path:
        return "No download folder recorded"
    if not os.path.isdir(path):
        return f"Folder does not exist: {path}"
    if not library.find_audio_files(path):
        return f"No audio files found in {path}"
    return ""


def enqueue(dl: Download):
    dl.m4b_status = "queued"
    dl.m4b_progress = None
    dl.error_message = None
    dl.queued_at = datetime.utcnow()


def apply_candidate(meta: dict, candidate: dict):
    """Copy Audible fields into saved metadata, keeping forum values where Audible has none."""
    for key in ("title", "subtitle", "author", "narrator", "series", "series_part", "year", "publisher",
                "description", "asin", "isbn", "language", "genres", "cover_url"):
        value = candidate.get(key)
        if value:
            meta[key] = value
    meta["match_confidence"] = candidate.get("confidence", meta.get("match_confidence"))


def merge_command(input_path: str, output_file: str, meta: dict, cover: str, jobs: str, bitrate: str) -> list[str]:
    cmd = [
        "m4b-tool", "merge", input_path,
        f"--output-file={output_file}",
        f"--include-extensions={MERGE_EXTENSIONS}",
        "--no-interaction",
    ]
    if meta.get("title"):
        cmd += [f"--name={meta['title']}", f"--album={meta['title']}"]
    if meta.get("author"):
        cmd += [f"--artist={meta['author']}", f"--albumartist={meta['author']}"]
    if meta.get("narrator"):
        cmd.append(f"--writer={meta['narrator']}")  # composer tag — ABS reads it as the narrator
    if meta.get("series"):
        cmd.append(f"--series={meta['series']}")
        if meta.get("series_part"):
            cmd.append(f"--series-part={meta['series_part']}")
    if meta.get("year"):
        cmd.append(f"--year={meta['year']}")
    if meta.get("genres"):
        cmd.append(f"--genre={meta['genres'][0]}")
    if meta.get("description"):
        cmd += [f"--description={meta['description'][:250]}", f"--longdesc={meta['description']}"]
    if cover:
        cmd.append(f"--cover={cover}")
    if jobs:
        cmd.append(f"--jobs={jobs}")
    if bitrate:
        cmd.append(f"--audio-bitrate={bitrate}")
    return cmd


def tone_command(file_path: str, meta: dict, cover: str) -> list[str]:
    cmd = ["tone", "tag", file_path]
    if meta.get("title"):
        cmd += [f"--meta-title={meta['title']}", f"--meta-album={meta['title']}"]
    if meta.get("author"):
        cmd += [f"--meta-artist={meta['author']}", f"--meta-album-artist={meta['author']}"]
    if meta.get("narrator"):
        cmd += [f"--meta-narrator={meta['narrator']}", f"--meta-composer={meta['narrator']}"]
    if meta.get("series"):
        cmd.append(f"--meta-movement-name={meta['series']}")
        if meta.get("series_part"):
            cmd.append(f"--meta-part={meta['series_part']}")
    if meta.get("genres"):
        cmd.append(f"--meta-genre={meta['genres'][0]}")
    if meta.get("description"):
        cmd += [f"--meta-description={meta['description'][:250]}", f"--meta-long-description={meta['description']}"]
    if cover:
        cmd.append(f"--meta-cover-file={cover}")
    return cmd


def find_duplicates(db, title: str, author: str = "", nzb_hash: str = "", search_term: str = "",
                    exclude_id: int = None) -> list[str]:
    """Reasons this book may already be downloaded or in the library (empty list if none)."""
    from app.services.matching import ratio

    reasons = []
    q = db.query(Download)
    if exclude_id:
        q = q.filter(Download.id != exclude_id)
    filters = [Download.nzb_hash == nzb_hash] if nzb_hash else []
    if search_term:
        filters.append(Download.search_term == search_term)
    exact = q.filter(or_(*filters)).order_by(Download.created_at.desc()).first() if filters else None
    if exact:
        when = exact.created_at.strftime("%Y-%m-%d") if exact.created_at else "earlier"
        reasons.append(f"This NZB was already sent on {when} (download #{exact.id}, {exact.status})")
    elif title:
        for dl in q.filter(Download.status != "failed").order_by(Download.created_at.desc()).limit(500):
            meta = dl.parsed_metadata
            if ratio(title, meta.get("title") or dl.display_title) >= 0.9 and (
                    not author or not meta.get("author") or ratio(author, meta["author"]) >= 0.6):
                reasons.append(f"'{dl.display_title}' is already in your download history (#{dl.id})")
                break

    if title and abs_configured(db):
        try:
            client = AbsClient(get_setting(db, "abs_url"), get_setting(db, "abs_token"))
            for item in client.search_library(get_setting(db, "abs_library_id"), title):
                md = (item.get("media") or {}).get("metadata") or {}
                item_author = md.get("authorName") or ", ".join(a.get("name", "") for a in md.get("authors") or [])
                if ratio(title, md.get("title", "")) >= 0.9 and (
                        not author or not item_author or ratio(author, item_author) >= 0.6 or
                        normalize(author) in normalize(item_author)):
                    reasons.append(f"Already in Audiobookshelf: '{md.get('title')}' by {item_author or 'unknown author'}")
                    break
        except Exception as exc:
            logger.warning("Duplicate check against Audiobookshelf failed: %s", exc)
    return reasons


pipeline = Pipeline()
