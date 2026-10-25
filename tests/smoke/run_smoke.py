"""
End-to-end conversion check, run inside the built Docker image by CI:

  docker run --rm -e PUID=1000 -e PGID=1000 -v "$PWD/tests:/tests:ro" bookclub:ci python /tests/smoke/run_smoke.py

Generates a fake three-part audiobook with ffmpeg, runs the real pipeline
(m4b-tool + tone), and checks the .m4b, metadata.json, cover, chapters and
tags that Audiobookshelf relies on. Exits non-zero on any failure.
"""
import asyncio
import json
import os
import shutil
import subprocess
import sys

ROOT = "/tmp/bookclub-smoke"
shutil.rmtree(ROOT, ignore_errors=True)
os.makedirs(ROOT)
os.environ["DATABASE_URL"] = f"sqlite:///{ROOT}/smoke.db"
os.environ["BOOKCLUB_DISABLE_PIPELINE"] = "1"
os.chdir("/app")
sys.path.insert(0, "/app")

from app.main import app  # noqa: E402,F401 — creates tables
from app.database import SessionLocal  # noqa: E402
from app.models import Download  # noqa: E402
from app.services.pipeline import Pipeline, enqueue  # noqa: E402
from app.settings_store import set_setting  # noqa: E402

DOWNLOADS = f"{ROOT}/downloads"
LIBRARY = f"{ROOT}/library"
failures = []


def check(condition, message):
    print(("PASS " if condition else "FAIL ") + message)
    if not condition:
        failures.append(message)


def sh(*cmd):
    subprocess.run(cmd, check=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)


def make_book(folder: str, parts: int):
    os.makedirs(folder, exist_ok=True)
    for i in range(1, parts + 1):
        sh("ffmpeg", "-y", "-f", "lavfi", "-i", f"sine=frequency={300 + i * 100}:duration=6",
           "-ac", "1", "-ar", "22050", "-b:a", "32k", "-metadata", f"title=Chapter {i}",
           f"{folder}/{i:02d} - Chapter {i}.mp3")
    sh("ffmpeg", "-y", "-f", "lavfi", "-i", "color=c=navy:s=300x300", "-frames:v", "1", f"{folder}/cover.jpg")


def tone_dump(path: str) -> dict:
    out = subprocess.run(["tone", "dump", path, "--format", "json"], check=True, capture_output=True, text=True)
    return json.loads(out.stdout)


def convert(meta: dict, source: str) -> Download:
    db = SessionLocal()
    dl = Download(status="downloaded", download_path=source, download_metadata=json.dumps({**meta, "input_path": source}))
    db.add(dl)
    db.commit()
    enqueue(dl)
    db.commit()
    dl_id = dl.id
    db.close()
    asyncio.run(Pipeline().convert(dl_id))
    db = SessionLocal()
    dl = db.get(Download, dl_id)
    db.close()
    return dl


db = SessionLocal()
for key, value in {
    "m4b_output_path": f"{ROOT}/m4b",
    "local_path_prefix": DOWNLOADS,
    "m4b_move_template": LIBRARY + "/[Author]/[Series]/[BookNumber] - [Title]",
    "m4b_bitrate": "32k",
    "delete_source": "1",
}.items():
    set_setting(db, key, value)
db.close()

print(f"Running as uid={os.getuid()} gid={os.getgid()}")

# ── 1. three MP3s → merged, tagged M4B ───────────────────────────────────────
source = f"{DOWNLOADS}/Smoke Book"
make_book(source, 3)
meta = {"title": "The Smoke Test", "author": "Test Author", "narrator": "Test Narrator",
        "series": "Smoke Series", "series_part": "2", "year": "2021", "genres": ["Fantasy"],
        "description": "A book used to check the BookClub pipeline.", "asin": "B0SMOKE"}
dl = convert(meta, source)
expected = f"{LIBRARY}/Test Author/Smoke Series/2 - The Smoke Test/The Smoke Test.m4b"
check(dl.m4b_status == "converted", f"merge converted (status={dl.m4b_status}, error={dl.error_message})")
if dl.m4b_status != "converted":
    print(dl.conversion_log)
check(dl.m4b_path == expected, f"filed at ABS layout path ({dl.m4b_path})")
check(os.path.isfile(expected) and os.path.getsize(expected) > 10_000, "m4b exists and is not empty")
check(not os.path.exists(source), "source folder deleted after success")

book_dir = os.path.dirname(expected)
if os.path.isfile(f"{book_dir}/metadata.json"):
    md = json.load(open(f"{book_dir}/metadata.json", encoding="utf-8"))
    check(md.get("series") == ["Smoke Series #2"], f"metadata.json series ({md.get('series')})")
    check(md.get("narrators") == ["Test Narrator"], f"metadata.json narrators ({md.get('narrators')})")
    check(md.get("asin") == "B0SMOKE", "metadata.json asin")
else:
    check(False, "metadata.json written")
check(os.path.isfile(f"{book_dir}/cover.jpg"), "cover.jpg written next to the book")

if os.path.isfile(expected):
    dump = tone_dump(expected)
    print(json.dumps(dump.get("meta", dump), indent=1)[:3000])
    tags = dump.get("meta", {})
    text = json.dumps(dump)
    check(tags.get("title") == "The Smoke Test" or tags.get("album") == "The Smoke Test", "title/album tag")
    check(tags.get("artist") == "Test Author", f"artist tag ({tags.get('artist')})")
    check("Test Narrator" in text, "narrator/composer tag")
    check("Smoke Series" in text, "series tag")
    check(len(tags.get("chapters") or []) == 3, f"3 chapters ({len(tags.get('chapters') or [])})")
    check(bool(tags.get("embeddedPictures")), "embedded cover art")

# ── 2. a single existing M4B → copied and re-tagged with tone ────────────────
if os.path.isfile(expected):
    single_src = f"{DOWNLOADS}/Single File Book"
    os.makedirs(single_src)
    shutil.copy(expected, f"{single_src}/whatever.m4b")
    dl2 = convert({"title": "Second Book", "author": "Other Author", "narrator": "Second Narrator"}, single_src)
    expected2 = f"{LIBRARY}/Other Author/Second Book/Second Book.m4b"
    check(dl2.m4b_status == "converted", f"single m4b handled (status={dl2.m4b_status}, error={dl2.error_message})")
    check(dl2.m4b_path == expected2, f"standalone layout ({dl2.m4b_path})")
    if os.path.isfile(expected2):
        tags2 = tone_dump(expected2).get("meta", {})
        check(tags2.get("title") == "Second Book", f"tone re-tagged title ({tags2.get('title')})")
        check(tags2.get("artist") == "Other Author", f"tone re-tagged artist ({tags2.get('artist')})")

# ── 3. refusing to overwrite ─────────────────────────────────────────────────
dup_src = f"{DOWNLOADS}/Duplicate"
make_book(dup_src, 1)
dl3 = convert(meta, dup_src)
check(dl3.m4b_status == "m4b_failed" and "already exists" in (dl3.error_message or ""), "existing book is not overwritten")
check(os.path.isdir(dup_src), "source kept when nothing was converted")

# ── 4. (informational) non-ASCII file names, as in Stephen King's It ─────────
# Reported only — it must not block publishing while the cause is investigated.
accents_src = f"{DOWNLOADS}/Accents"
make_book(accents_src, 2)
os.rename(f"{accents_src}/01 - Chapter 1.mp3", f"{accents_src}/01 - The Ritual of Chüd.mp3")
os.rename(f"{accents_src}/02 - Chapter 2.mp3", f"{accents_src}/02 - The Ritual of ChÃ¼d.mp3")
dl4 = convert({"title": "Accents Book", "author": "Test Author"}, accents_src)
print(f"INFO non-ASCII file names: status={dl4.m4b_status} error={dl4.error_message}")
if dl4.m4b_status != "converted":
    print(dl4.conversion_log)

print()
if failures:
    print(f"{len(failures)} check(s) failed:")
    for f in failures:
        print(" -", f)
    sys.exit(1)
print("All smoke checks passed")
