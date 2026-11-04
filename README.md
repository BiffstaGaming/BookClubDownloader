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
  - A book waiting for you is labelled *Best guess* (the closest Audible match,
    not yet confirmed) and its button says *Review & convert*.
  - When NZBGet fails a download with missing articles, damage or an unpack
    failure, BookClub sends the next-best upload of the same post by itself
    (*Settings → NZBGet → try another NZB*, 2 times by default). NZBs are chosen
    M4B first, then complete (largest) and newest. A wrong password is not retried.
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
- Pages update **live**: the server tells open pages what changed (one connection
  per browser, shared by all its BookClub tabs), so progress appears straight away
  and nothing is polled. A tab in the background does nothing until you come back
  to it. Behind a reverse proxy, turn off response buffering for `/events` (or
  BookClub falls back to checking every 10 seconds).
- Bootstrap, the icons and htmx are served by BookClub itself, so the pages work
  without internet access. Covers are kept on BookClub's disk after the first view.

## Wishlist and series

**Wishlist** (in the menu) watches the forum for you, every 12 hours by default
(*Settings → Background Processing*):

- **A book** that isn't on the forum yet: requested as soon as a topic matching it
  appears.
- **An author**: new uploads by them (what's already there when you start watching
  isn't news).
- **A series**: every book of it you don't have, as soon as it's on the forum,
  lowest number first. "Have" means it's in Audiobookshelf or your download history.

With *Download automatically* off, found books are listed with a *Get* button and
announced (`wishlist_found` event) instead. Found books go through the normal
request, so a book you already have is never downloaded twice.

The **Series in your library** tab lists every series in Audiobookshelf with the
numbers you have and the gaps (*you have 1–3 and 6*). *What's on the forum?* shows
the series' books on the forum with *Get* buttons; *Follow* adds it to the wishlist.

## Library health

**Library** (in the menu) reads your whole Audiobookshelf library and lists books
with no cover, never matched to Audible, possible duplicates, series without a book
number or with two books on the same number, low-bitrate audio, books made of many
files, and books with no author. *Quick Match* fills empty details and the cover
from Audible. *Ask Claude to suggest fixes* (with the AI reviewer set up) proposes
series numbers and authors; nothing changes until you press *Approve* on each one.

## Books that aren't on the forum (Prowlarr, qBittorrent)

*Settings → Other Indexers & Torrents*: with **Prowlarr** connected, the Search page
gets a *Not on the forum?* box that searches all your usenet and torrent indexers
at once (audiobook category), best match first. Usenet results go to NZBGet;
torrent results go to **qBittorrent** (with its own path mapping). Requests from Home
Assistant also try Prowlarr when the forum has nothing, and take a result only when
it clearly is the requested book. Torrent downloads are converted like any other
book, but their folder is never deleted: qBittorrent keeps seeding it.

## On your phone

Phones get a separate layout at `/m` (opening any BookClub page on a phone goes
there): a bottom tab bar with **Home** (get a book, what's happening now, recently
added), **Search**, **Downloads** (cards, plus a *Needs me* filter), **Wishlist** and
**More** (Saved posts, Library health, Logs, Settings). Panels such as *Convert* open full screen, and the Back button closes
them. In Chrome's menu, *Add to Home screen* gives it an app icon. *More → Use the
desktop view* switches back to the full pages; *Phone view* in the desktop menu
switches again.

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
  - **Only start encoding between** e.g. 01:00 and 07:00 holds MP3 books for the
    night; books that are only copied or joined still go straight away. Uses the
    time zone in Settings (filled in from your browser).
  - **Pause encoding when the CPU reaches** e.g. 80 °C freezes the encoder (it isn't
    stopped) until the CPU is 5 °C cooler, and holds new encodes while it's hot. The
    Settings page shows the current reading; inside Docker the CPU sensors under
    `/sys/class/hwmon` are normally visible without any extra setup.
- **Chapters**: when a book's chapters are only "Chapter 1, 2, 3" or file names and
  it has an Audible ASIN, BookClub writes Audible's real chapter names (from
  Audnexus) — only when the running time matches, so they start in the right
  places. *Settings → Audiobookshelf & Matching → Real chapter names*.

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
`download_completed`, `download_failed`, `download_retrying`, `conversion_failed`,
`needs_review`, `abs_import_failed`, `request_sent`, `request_needs_choice`,
`request_not_found`, `request_duplicate`, `request_failed`, `wishlist_found`, `test`.

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

## Home Assistant integration (HACS)

Instead of the YAML above, Home Assistant can talk to BookClub through its own
integration (`custom_components/bookclub`, Home Assistant 2024.8 or newer).

Install: *HACS → ⋮ → Custom repositories*, add
`https://github.com/BiffstaGaming/Book-Club-Downloader` with category
*Integration*, download **BookClub** and restart Home Assistant. Then *Settings →
Devices & services → Add integration → BookClub*, enter the BookClub address
(e.g. `http://192.168.1.10:9933`) and the API token from *Settings → Home
Assistant*. The integration's options set the update interval (30 s), whether
BookClub sends its events to Home Assistant, and whether to install the voice
sentences.

**Sensors** (one BookClub device): Status (`converting`, `downloading`, `queued`,
`searching`, `attention`, `idle`, with the queue, current conversion and counts as
attributes), Downloading, Waiting to convert, Converting, Conversion progress,
Needs a manual match, Failed, Imported in the last 24 hours, Last converted, Last
imported, Last import (time), Requests searching, Requests waiting for a choice,
Wishlist watching, Wishlist found, and diagnostic AI calls today, AI plan used (5
hours) and Last download check. A **Needs attention** binary sensor is on while
anything failed, needs a manual match or waits for a choice.

**Actions**: `bookclub.request_book` (`query`, optional `requested_by`, `force`;
returns the request, with its `id`, when called with a response),
`bookclub.choose_option` (`request_id`, `option`), `bookclub.cancel_request`
(`request_id`) and `bookclub.add_to_wishlist` (`query`, `kind`: book/author/series,
optional `author`, `auto_download`). With more than one BookClub server set up,
pass `config_entry_id`.

**Events**: the integration registers its own webhook with BookClub, and every
BookClub event arrives on the event bus as `bookclub_event` with the payload
fields listed under *Notifications* (plus `config_entry_id`) — no webhook
automation needed:

```yaml
triggers:
  - trigger: event
    event_type: bookclub_event
    event_data:
      event: book_imported
actions:
  - action: notify.notify
    data:
      title: "New audiobook: {{ trigger.event.data.title }}"
      message: "{{ trigger.event.data.author }}"
```

The webhook only accepts calls from the local network, so BookClub must reach Home
Assistant's local URL (*Settings → System → Network*).

**Voice**: the integration adds two Assist intents, `BookClubRequestBook` ("download
the audiobook Project Hail Mary") and `BookClubStatus` ("what is BookClub doing"),
and writes `custom_sentences/en/bookclub.yaml` for them. It never overwrites a
`bookclub.yaml` it didn't write — if you made one from the *Voice* example above,
delete it (and the `intent_script`) to use the integration's sentences. AI
conversation agents with *Assist* control can use both intents directly.

Tests: `pip install pytest-homeassistant-custom-component hassil`, then
`pytest tests_ha` (`.github/workflows/home-assistant.yml` also runs hassfest and
the HACS check).
