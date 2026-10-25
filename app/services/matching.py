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


def compute_confidence(nzb_title: str, nzb_author: str, audible_title: str, audible_author: str) -> int:
    """
    Return 0-100 match confidence.
    Title is the decisive factor — if it scores < 50%, author match cannot save it.
    Above 50% title similarity, author (35%) is factored in.
    """
    title_score = ratio(nzb_title, audible_title)
    if title_score < 0.5:
        return round(title_score * 100)
    if nzb_author and audible_author:
        # Audible joins multiple authors with commas; score against the best one
        author_score = max(ratio(nzb_author, a) for a in [audible_author, *audible_author.split(",")])
        score = title_score * 0.65 + author_score * 0.35
    else:
        score = title_score
    return round(score * 100)


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
                    preferred_language: str = "") -> list[dict]:
    """
    Normalised candidates, best first, each with a `confidence` score.
    Editions in a language other than `preferred_language` are scored down
    (and flagged `language_mismatch`) so they sort below — and never auto-convert
    ahead of — an edition in the right language.
    """
    ranked = []
    for book in results[:25]:
        fields = audible_fields(book)
        confidence = compute_confidence(title, author, fields["title"], fields["author"])
        fields["language_mismatch"] = language_mismatch(fields["language"], preferred_language)
        if fields["language_mismatch"]:
            confidence = max(0, min(confidence - LANGUAGE_MISMATCH_PENALTY, LANGUAGE_MISMATCH_CAP))
        fields["confidence"] = confidence
        ranked.append(fields)
    ranked.sort(key=lambda c: c["confidence"], reverse=True)
    return ranked[:limit]
