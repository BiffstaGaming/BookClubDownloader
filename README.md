# BookClub

Self-hosted audiobook pipeline: find a book on the abook.link forum, grab the NZB,
let NZBGet download it, convert it to a single tagged `.m4b`, file it into your
Audiobookshelf library and have ABS import it — without babysitting a browser tab.

```
Forum search → Thank to reveal → NZB search (NZBKing, falls back to Binsearch)
  → NZBGet download → Audible match (via ABS) → m4b-tool merge + tone tags
  → Author/Series/N - Title/Title.m4b + metadata.json + cover.jpg
  → Audiobookshelf scan + Quick Match → Home Assistant notification
```

## Running it

Use [`compose.yaml`](compose.yaml) (works as a Portainer stack). The image is
published to `ghcr.io/biffstagaming/bookclub` by CI on every push to `main`, after
the unit tests and a real conversion smoke test pass.

| Environment | Default | Meaning |
|---|---|---|
| `PUID` / `PGID` | root | Run as this user/group so files match Audiobookshelf/NZBGet ownership |
| `UMASK` | `002` | Permissions for new files and folders |

Volumes: `/app/data` (database), your NZBGet completed-downloads folder, and the
media share that holds the Audiobookshelf library.

## Setting it up

Open `http://<host>:9933/settings`.

1. **abook.link** — forum login. Use *Test* to check it.
2. **NZBGet** — URL and credentials. If NZBGet sees the downloads folder under a
   different path than BookClub, set *NZBGet Path Prefix* (NZBGet's view) and
   *Local Path Prefix* (BookClub's view, e.g. `/downloads`). BookClub only ever
   deletes folders inside these paths.
3. **Paths & Conversion**
   - *M4B Output Directory*: m4b-tool's working folder (and the library root if no
     template is set).
   - *Library Location Template*: where finished books go. Recommended:
     `/Media/Audiobooks/[Author]/[Series]/[BookNumber] - [Title]/[Title].m4b`.
     Empty parts are dropped, so a standalone book becomes `Author/Title/Title.m4b`.
     Audiobookshelf needs **each book in its own folder** — the page warns you if a
     template would put several books in one folder.
   - *Check Folders* confirms each path exists inside the container and is writable.
4. **Audiobookshelf & Matching**
   - Create an **API key** in ABS (*Settings → API Keys*, ABS 2.26+) for an admin
     user. Old user tokens still work but ABS has deprecated them.
   - *Fetch Libraries* and pick your audiobook library.
   - If ABS sees the library at a different path, fill in the path mapping
     (e.g. `/Media/Audiobooks` in BookClub = `/audiobooks` in ABS).
   - Pick the Audible region, the auto-convert confidence and whether to convert
     automatically.
   - Recommended in ABS: enable the **folder watcher** for the library (BookClub
     asks it to scan just the new book; without it BookClub falls back to a full
     library scan).
5. **Home Assistant** — see below.

## Day to day

- **Search** → pick a topic → *Thank to Reveal*. The NZB search starts by itself;
  if the book already exists in your download history or in Audiobookshelf you get
  a warning before anything is sent.
- **Downloads** shows progress. Everything after *Send to NZBGet* happens in the
  background — the page can be closed.
  - A confident Audible match (≥ the threshold) converts automatically. Otherwise
    the row says *Needs review*: click *Convert*, then *Find on Audible* to pick
    the right edition from the list (covers, narrator, series, length).
  - Failed downloads stay listed with the reason (wrong password, missing
    articles, …) and *Retry* / *Other NZB* buttons.
  - Conversions run one at a time; *Dequeue* or *Stop* as needed. A restart
    resumes the queue.
  - Once imported, the row links straight to the book in Audiobookshelf.
- **Logs** keeps per-book detail including full m4b-tool output (pruned after
  the number of days set in Settings).

## Home Assistant

### Notifications (webhook)

Create an automation, copy its webhook URL into *Settings → Home Assistant* and
press *Send Test Event*:

```yaml
alias: BookClub notifications
triggers:
  - trigger: webhook
    webhook_id: bookclub-change-me-to-something-random
    allowed_methods: [POST]
    local_only: true
actions:
  - action: notify.notify          # or notify.mobile_app_<your_phone>
    data:
      title: "BookClub: {{ trigger.json.event_label }}"
      message: >-
        {{ trigger.json.title }}{% if trigger.json.author %} by {{ trigger.json.author }}{% endif %}
        {% if trigger.json.message %} — {{ trigger.json.message }}{% endif %}
      data:
        url: "{{ trigger.json.url }}"
```

The webhook URL is `http://<home-assistant>:8123/api/webhook/<webhook_id>`.

Events (`trigger.json.event`): `book_imported`, `book_converted`,
`download_completed`, `download_failed`, `conversion_failed`, `needs_review`,
`abs_import_failed`, `test`. Payload fields: `event`, `event_label`, `title`,
`author`, `download_id`, `message`, `url`, `timestamp`.

### Status sensor

```yaml
# configuration.yaml
rest:
  - resource: http://<bookclub-host>:9933/api/status
    scan_interval: 30
    sensor:
      - name: BookClub
        icon: mdi:book-music
        value_template: "{{ value_json.state }}"
        json_attributes:
          - downloading
          - queued
          - converting
          - converting_progress
          - needs_review
          - failed
          - imported_last_24h
          - last_converted
          - queue
          - nzbget_reachable
```

`state` is one of `converting`, `downloading`, `queued`, `attention` (failures or
books waiting for a manual match) or `idle`.

## Development

```bash
pip install -r requirements.txt pytest httpx
pytest -q
uvicorn app.main:app --reload --port 9933
```

`tests/smoke/run_smoke.py` runs a real m4b-tool + tone conversion and is executed
inside the built image by CI (`.github/workflows/release.yml`).
