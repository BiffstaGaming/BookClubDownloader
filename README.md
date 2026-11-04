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

Saved passwords and tokens are never shown again; leave a field blank to keep it,
or tick *Forget the saved value* to remove it. The web UI has no login, so keep it
on your LAN. Changes requested by other websites are refused, and the Home
Assistant API needs its own token.

1. **abook.link** — forum login. Use *Test* to check it.
2. **NZBGet** — URL and credentials. If NZBGet sees the downloads folder under a
   different path than BookClub, set *NZBGet Path Prefix* (NZBGet's view) and
   *Local Path Prefix* (BookClub's view, e.g. `/downloads`). BookClub only ever
   deletes folders inside these paths.
3. **Paths & Conversion**
   - *M4B Output Directory*: m4b-tool's working folder (and the library root if no
     template is set). The default `/m4b` lives inside the container, which is fine
     as a working folder when a library template is set. Without a template, point
     it at a mounted folder, or finished books are lost on the next update.
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
6. **AI Reviewer** (optional) — see below.

## Day to day

- **Search** → pick a topic → *Thank to Reveal*. The NZB search starts by itself;
  if the book already exists in your download history or in Audiobookshelf you get
  a warning before anything is sent.
- **Downloads** shows progress. Everything after *Send to NZBGet* happens in the
  background — the page can be closed.
  - A confident Audible match (≥ the threshold) converts automatically. Subtitles
    and series labels don't count against a match, and the download's running
    time is compared with Audible's to rule out abridged or other editions. If
    Audible can't be reached, BookClub tries again later instead of giving up.
  - Otherwise the AI reviewer (if on) gets a look, and failing that the row says
    *Needs review* with the reason: click *Convert*, then *Find on Audible* to
    pick the right edition (covers, narrator, series, length).
  - Failed downloads stay listed with the reason (wrong password, missing
    articles, …) and *Retry* / *Other NZB* buttons.
  - Conversions run one at a time; *Dequeue* or *Stop* as needed. A restart
    resumes the queue.
  - Once imported, the row links straight to the book in Audiobookshelf.
- **Logs** keeps per-book detail including full m4b-tool output (pruned after
  the number of days set in Settings). The terminal button on a download row
  shows just that book's log.
- **Saved** lists every forum post BookClub has revealed: search term, password
  and book details, kept for good. *Send to NZBGet* finds the NZB for it and
  starts the download again (e.g. after losing files) without visiting the forum;
  *Choose NZB* lists every NZB to pick one. Posts behind your existing download
  history are added the first time this version starts.
- Forum searches are reused for a few hours (*Settings → abook.link → Reuse
  search results for*; a *Refresh from forum* button picks up new uploads), and a
  post revealed before opens from the saved copy. While you type, topics BookClub
  has already seen are suggested. All of this means fewer requests to the forum.
- The Search page, Downloads filters and Logs filters are remembered when you
  switch pages or tabs. Times are shown in your browser's timezone.

## Encoding speed

Each finished book's log line says how it was converted and how long it took,
e.g. *encoded, 11h 32m of audio in 3m 10s (219× real time)*.

- Books that are already AAC (`.m4b`/`.m4a`) are **joined without re-encoding**:
  seconds, not minutes. A single `.m4b` is just copied and re-tagged.
- MP3 books must be encoded. *Settings → Paths & Conversion → Encoding speed & CPU*:
  - **Mono, 22.05 kHz, 64k** (the defaults) is about a quarter of the work of
    44.1 kHz stereo and sounds the same for a narrator.
  - **Parallel jobs** encodes several files of a multi-file book at once. That is
    the main speed/heat trade-off: each job keeps about one core busy.
  - **Use at most N cores** pins encoding to that many cores. The encoder always
    runs at low priority, so other services stay responsive. Fewer cores run
    cooler but take longer.

## AI reviewer (optional, on your Claude plan)

When the rules can't match a download to Audible confidently, Claude compares the
candidates (title, author, narrator, running time) and picks the edition, or says
none fits. It can also pick the forum topic for a voice request. Its pick still
has to pass the language and running-time checks and the confidence threshold.

It runs through the Claude Code CLI inside the container on **your Claude
subscription**, not the paid API (the same setup as DealFinder):

1. On any computer with Claude Code: `claude setup-token`, then sign in.
2. Paste the token in *Settings → AI Reviewer* (or set `CLAUDE_CODE_OAUTH_TOKEN`
   in `compose.yaml`), tick **Ask Claude when a match is uncertain**, then
   *Test Claude*.

The calls count against your plan's limits. BookClub stops calling Claude after
*N calls a day*, and pauses while your 5-hour or 7-day usage is above the
percentage you set. Claude runs with no tools and no connectors, and a reply is
only used if it names one of the options BookClub sent.

## Home Assistant

Set *Settings → Home Assistant → BookClub address for links* (e.g.
`http://192.168.1.10:9933`) so notifications can link straight to a book.

### Notifications (webhook)

Create an automation, copy its webhook URL into *Settings → Home Assistant*, pick
the events you want and press *Send Test Event*:

```yaml
alias: BookClub notifications
triggers:
  - trigger: webhook
    webhook_id: bookclub-change-me-to-something-random
    allowed_methods: [POST]
    local_only: true
actions:
  - choose:
      # A voice request found several books: ask on the phone, with a button each.
      - conditions: "{{ trigger.json.event == 'request_needs_choice' }}"
        sequence:
          - action: notify.mobile_app_janets_phone
            data:
              title: "Which book did you mean?"
              message: "{{ trigger.json.title }}"
              data:
                actions: >-
                  [{% for o in trigger.json.options[:3] %}{"action": "BOOKCLUB_{{ trigger.json.request_id }}_{{ o.option }}",
                  "title": {{ (o.title | truncate(45)) | tojson }}}{{ "," if not loop.last }}{% endfor %}]
      # A finished book: say it on the speaker too.
      - conditions: "{{ trigger.json.event == 'book_imported' }}"
        sequence:
          - action: notify.mobile_app_janets_phone
            data:
              title: "New audiobook: {{ trigger.json.title }}"
              message: "{{ trigger.json.author }}{% if trigger.json.narrator %}, read by {{ trigger.json.narrator }}{% endif %}"
              data:
                url: "{{ trigger.json.url }}"
                image: "{{ trigger.json.cover_url }}"
          - action: tts.speak
            target: { entity_id: tts.home_assistant_cloud }
            data:
              media_player_entity_id: media_player.kitchen
              message: "{{ trigger.json.title }} is ready in Audiobookshelf."
    default:
      - action: notify.notify
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
`abs_import_failed`, `request_sent`, `request_needs_choice`, `request_not_found`,
`request_duplicate`, `request_failed`, `test`.

Payload fields: `event`, `event_label`, `title`, `author`, `narrator`, `series`,
`series_part`, `cover_url`, `download_id`, `request_id`, `requested_by`,
`message`, `url` (the Audiobookshelf item when there is one, otherwise BookClub),
`bookclub_url`, `abs_url`, `options` (for `request_needs_choice`: `option`,
`topic_id`, `title`), `timestamp`.

### "Find and download this book" (requests)

Home Assistant can ask BookClub to find a book and send it to NZBGet: it searches
the forum, picks the topic (asking Claude if that's unclear and the AI reviewer is
on), thanks the post to reveal it, finds the NZB and sends it. From there the
normal pipeline converts and imports it, and `book_imported` fires with the same
`requested_by`. Requests also show (and can be typed) at the top of the Downloads
page.

Copy the API token from *Settings → Home Assistant*, then:

```yaml
# secrets.yaml
bookclub_auth: "Bearer <API token from BookClub Settings>"

# configuration.yaml
rest_command:
  bookclub_request:
    url: http://<bookclub-host>:9933/api/requests
    method: POST
    headers:
      Authorization: !secret bookclub_auth
    content_type: application/json
    payload: '{"query": {{ query | tojson }}, "requested_by": {{ (requested_by | default("")) | tojson }}}'
  bookclub_choose:
    url: "http://<bookclub-host>:9933/api/requests/{{ request_id }}/choose"
    method: POST
    headers:
      Authorization: !secret bookclub_auth
    content_type: application/json
    payload: '{"option": {{ option | int }}}'
```

**Voice** (Assist): a custom sentence with a free-text slot, and an intent script.

```yaml
# config/custom_sentences/en/bookclub.yaml
language: en
intents:
  BookClubRequest:
    data:
      - sentences:
          - "(download|get|find) [the] [audio]book {book}"
          - "add [the] [audio]book {book} to [the] library"
lists:
  book:
    wildcard: true

# configuration.yaml
intent_script:
  BookClubRequest:
    action:
      - action: rest_command.bookclub_request
        data:
          query: "{{ book }}"
          requested_by: "voice"
    speech:
      text: "Okay, I'm looking for {{ book }}. I'll tell you when it's ready."
```

"Download the audiobook Project Hail Mary by Andy Weir" then works from any voice
assistant. If you use an AI conversation agent in Home Assistant, expose a script
that calls `rest_command.bookclub_request` instead, and any phrasing works.

**Answering "which book?" from the phone notification** (the buttons from the
automation above):

```yaml
alias: BookClub choice
triggers:
  - trigger: event
    event_type: mobile_app_notification_action
conditions:
  - "{{ trigger.event.data.action.startswith('BOOKCLUB_') }}"
actions:
  - action: rest_command.bookclub_choose
    data:
      request_id: "{{ trigger.event.data.action.split('_')[1] }}"
      option: "{{ trigger.event.data.action.split('_')[2] }}"
```

API: `POST /api/requests` `{"query", "requested_by", "force"}` → 202 with the
request; `GET /api/requests`, `GET /api/requests/{id}`;
`POST /api/requests/{id}/choose` `{"option"}` or `{"topic_id"}` or
`{"force": true}` (for a duplicate); `POST /api/requests/{id}/cancel`. All need
`Authorization: Bearer <token>`.

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
          - last_imported
          - last_imported_at
          - requests_searching
          - requests_waiting
          - queue
          - nzbget_reachable
          - ai_calls_today
          - ai_plan_5h_percent
```

`state` is one of `converting`, `downloading`, `queued`, `searching` (a request is
being worked on), `attention` (failures, books waiting for a manual match, or a
request waiting for a choice) or `idle`. `/api/status` needs no token.

## Development

```bash
pip install -r requirements.txt pytest httpx
pytest -q
uvicorn app.main:app --reload --port 9933
```

`tests/smoke/run_smoke.py` runs a real m4b-tool + tone conversion and is executed
inside the built image by CI (`.github/workflows/release.yml`).
