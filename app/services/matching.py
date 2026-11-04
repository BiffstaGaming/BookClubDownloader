"""Matching NZB / forum names against Audible results (pure functions)."""
import difflib
import re


def parse_nzb_name(name: str) -> tuple[str, str]:
    """
    Parse the forum NZB naming convention:
      "Book Club - [FLAG] Author - Series## - BookTitle (Narrator/Year)"
    Returns (title, author).
    Strips any trailing parenthetical (year OR narrator name).
    Strips bracketed flags like [SPOT] from the author segment.
    """
    name = re.sub(r'\s*\([^)]+\)\s*$', '', name or "").strip()
    parts = [p.strip() for p in name.split(' - ')]
    if len(parts) >= 3:
        title  = parts[-1]
        author = re.sub(r'^\[[^\]]+\]\s*', '', parts[1]).strip()
    elif len(parts) == 2:
        title  = parts[-1]
        author = ""
    else:
        title  = name
        author = ""
    return title, author


def normalize(s: str) -> str:
    return re.sub(r'\s+', ' ', re.sub(r'[^\w\s]', '', (s or "").lower())).strip()


def ratio(a: str, b: str) -> float:
    a, b = normalize(a), normalize(b)
    if not a or not b:
        return 0.0
    return difflib.SequenceMatcher(None, a, b).ratio()


# "(Unabridged)", "[Abridged]", "(Dramatized Adaptation)" never help a comparison.
_NOISE = re.compile(r"[(\[]\s*(?:un)?abridged\s*[)\]]|[(\[]\s*dramati[sz]ed[^)\]]*[)\]]", re.I)
# "Title: Subtitle", "Series - Title"
_SUBTITLE_SPLIT = re.compile(r"\s*:\s+|\s+[-–—]\s+")
# "Title, Book 3", "Title (Book One)", "Title - Volume 2"
_SERIES_SUFFIX = re.compile(
    r"[,(\[]?\s*\b(?:book|volume|vol\.?|part|episode)\s+(?:\d+(?:\.\d+)?|one|two|three|four|five|six|seven|"
    r"eight|nine|ten|eleven|twelve)\b.*$", re.I)

# "Leviathan Wakes (The Expanse, Book 1)"
_TRAILING_BRACKETS = re.compile(r"\s*[(\[][^()\[\]]*[)\]]\s*$")

# A match found only after dropping a subtitle or series marker is a little less certain.
VARIANT_FACTOR = 0.95


def title_variants(title: str) -> list[tuple[str, bool]]:
    """The title plus its shorter forms, each with True if it is the whole title."""
    full = _NOISE.sub("", title or "").strip()
    variants = [(full, True)]
    parts = [part for part in _SUBTITLE_SPLIT.split(full) if part.strip()]
    if len(parts) > 1:
        variants += [(parts[0], False), (parts[-1], False)]
    without_series = _SERIES_SUFFIX.sub("", full).strip(" ,-–—")
    if without_series and without_series != full:
        variants.append((without_series, False))
    without_brackets = _TRAILING_BRACKETS.sub("", full).strip(" ,-–—")
    if without_brackets and without_brackets != full:
        variants.append((without_brackets, False))
    return [(v, whole) for v, whole in variants if normalize(v)]


def title_similarity(a: str, b: str) -> float:
    """
    0-1 similarity of two book titles, tolerating subtitles and series markers on
    either side: "Dune" matches "Dune: Book One in the Dune Chronicles".
    """
    best = 0.0
    for x, x_whole in title_variants(a):
        for y, y_whole in title_variants(b):
            score = ratio(x, y)
            if not (x_whole and y_whole):
                score *= VARIANT_FACTOR
            best = max(best, score)
    return best


def author_similarity(wanted: str, candidate: str) -> float:
    """Audible joins several authors with commas; score against the best one."""
    if not wanted or not candidate:
        return 0.0
    names = [candidate, *re.split(r",|&|\band\b", candidate)]
    return max(ratio(wanted, name) for name in names if name.strip())


def looks_like_series(author: str) -> bool:
    """'Hunter Files 06' or 'Mistborn Book 1' is a series label, not a person."""
    text = (author or "").strip()
    return bool(re.search(r"\d$", text) or re.search(r"\b(?:book|vol|volume|series)\b", text, re.I))


def compute_confidence(nzb_title: str, nzb_author: str, audible_title: str, audible_author: str) -> int:
    """
    Return 0-100 match confidence.
    Title is the decisive factor — if it scores < 50%, author match cannot save it.
    Above 50% title similarity, author (35%) is factored in.
    """
    title_score = title_similarity(nzb_title, audible_title)
    if title_score < 0.5:
        return round(title_score * 100)
    if nzb_author and audible_author:
        score = title_score * 0.65 + author_similarity(nzb_author, audible_author) * 0.35
    else:
        score = title_score
    return round(score * 100)


# Running time is the best edition check there is: the same title abridged, or a
# different book, is a different length. Audible gives minutes.
DURATION_MATCH_PERCENT = 3
DURATION_MATCH_MINUTES = 5
DURATION_MISMATCH_PERCENT = 15
DURATION_BONUS = 5
DURATION_MISMATCH_CAP = 70


def duration_verdict(audio_minutes: float, audible_minutes: float) -> str:
    """'match', 'mismatch' or '' (unknown, or close enough not to matter either way)."""
    if not audio_minutes or not audible_minutes:
        return ""
    diff = abs(audio_minutes - audible_minutes)
    if diff <= DURATION_MATCH_MINUTES or diff / audible_minutes * 100 <= DURATION_MATCH_PERCENT:
        return "match"
    if diff / audible_minutes * 100 > DURATION_MISMATCH_PERCENT:
        return "mismatch"
    return ""


def audible_fields(book: dict) -> dict:
    """Normalise an ABS /api/search/books result into BookClub's metadata keys."""
    series_name, series_part = "", ""
    series_raw = book.get("series")
    if isinstance(series_raw, list) and series_raw:
        s = series_raw[0] or {}
        series_name = s.get("series") or s.get("name") or ""
        series_part = str(s.get("sequence") or s.get("position") or s.get("volumeNumber") or "")
    elif isinstance(series_raw, str):
        series_name = series_raw
        for k in ("volumeNumber", "sequence", "position", "seriesSequence"):
            if book.get(k):
                series_part = str(book[k])
                break
    genres = book.get("genres") or []
    if isinstance(genres, str):
        genres = [g.strip() for g in genres.split(",") if g.strip()]
    author = book.get("author") or book.get("authors") or ""
    if isinstance(author, list):
        author = ", ".join(a.get("name", a) if isinstance(a, dict) else str(a) for a in author)
    narrator = book.get("narrator") or ""
    if isinstance(narrator, list):
        narrator = ", ".join(str(n) for n in narrator)
    return {
        "title": book.get("title") or "",
        "subtitle": book.get("subtitle") or "",
        "author": author,
        "narrator": narrator,
        "series": series_name,
        "series_part": series_part,
        "year": str(book.get("publishedYear") or ""),
        "publisher": book.get("publisher") or "",
        "description": re.sub(r"<[^>]+>", "", book.get("description") or "").strip(),
        "asin": book.get("asin") or "",
        "isbn": book.get("isbn") or "",
        "language": book.get("language") or "",
        "genres": genres,
        "cover_url": book.get("cover") or "",
        "duration": book.get("duration") or 0,
    }


# Foreign-language editions often keep the original title, so title/author alone
# can score them 100%. A known language that differs from the preferred one loses
# this much and is capped below any auto-convert threshold.
LANGUAGE_MISMATCH_PENALTY = 40
LANGUAGE_MISMATCH_CAP = 60

_LANGUAGE_CODES = {
    "en": "english", "de": "german", "fr": "french", "es": "spanish", "it": "italian",
    "nl": "dutch", "pt": "portuguese", "sv": "swedish", "da": "danish", "no": "norwegian",
    "nb": "norwegian", "fi": "finnish", "pl": "polish", "ja": "japanese",
}


def normalize_language(language: str) -> str:
    lang = (language or "").strip().lower()
    return _LANGUAGE_CODES.get(lang, lang)


def language_mismatch(candidate_language: str, preferred_language: str) -> bool:
    """True when both languages are known and differ ("any" = no preference)."""
    preferred = normalize_language(preferred_language)
    candidate = normalize_language(candidate_language)
    return bool(preferred and preferred != "any" and candidate and candidate != preferred)


def rank_candidates(results: list, title: str, author: str = "", limit: int = 8,
                    preferred_language: str = "", audio_minutes: float = 0) -> list[dict]:
    """
    Normalised candidates, best first, each with a `confidence` score.
    Editions in a language other than `preferred_language` are scored down
    (and flagged `language_mismatch`) so they sort below — and never auto-convert
    ahead of — an edition in the right language. When the downloaded audio's
    length is known, a matching Audible length adds a little and a clearly
    different one caps the score below any auto-convert threshold.
    """
    ranked, seen = [], set()
    for book in results[:40]:
        fields = audible_fields(book)
        key = fields["asin"] or (normalize(fields["title"]), normalize(fields["author"]))
        if key in seen:
            continue  # several searches can return the same edition
        seen.add(key)
        confidence = compute_confidence(title, author, fields["title"], fields["author"])
        fields["language_mismatch"] = language_mismatch(fields["language"], preferred_language)
        if fields["language_mismatch"]:
            confidence = max(0, min(confidence - LANGUAGE_MISMATCH_PENALTY, LANGUAGE_MISMATCH_CAP))
        verdict = duration_verdict(audio_minutes, float(fields.get("duration") or 0))
        fields["duration_match"] = verdict == "match"
        fields["duration_mismatch"] = verdict == "mismatch"
        if verdict == "match":
            confidence = min(100, confidence + DURATION_BONUS)
        elif verdict == "mismatch":
            confidence = min(confidence, DURATION_MISMATCH_CAP)
        fields["confidence"] = confidence
        ranked.append(fields)
    ranked.sort(key=lambda c: (c["confidence"], c["duration_match"]), reverse=True)
    return ranked[:limit]
