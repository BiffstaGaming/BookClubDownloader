"""Subtitles, series labels and running time in Audible matching."""
from app.services.matching import (
    compute_confidence, duration_verdict, looks_like_series, rank_candidates, title_similarity,
)


def test_subtitles_and_series_labels_do_not_sink_the_score():
    for wanted, audible in [
        ("The Vatican Conspiracy", "The Vatican Conspiracy: A Joe Hawke Thriller"),
        ("Dune", "Dune: Book One in the Dune Chronicles"),
        ("Mistborn - The Final Empire", "The Final Empire"),
        ("Leviathan Wakes", "Leviathan Wakes (The Expanse, Book 1)"),
        ("Project Hail Mary", "Project Hail Mary (Unabridged)"),
    ]:
        assert compute_confidence(wanted, "A", audible, "A") >= 90, (wanted, audible)


def test_different_books_in_a_series_stay_apart():
    assert compute_confidence("The Way of Kings", "Brandon Sanderson", "Words of Radiance", "Brandon Sanderson") < 60
    assert compute_confidence("Red Rising", "Pierce Brown", "Golden Son", "Pierce Brown") < 60


def test_exact_title_beats_a_subtitle_match():
    assert title_similarity("Dune", "Dune") > title_similarity("Dune", "Dune: Book One")


def test_series_labels_are_not_authors():
    assert looks_like_series("Hunter Files 06")
    assert looks_like_series("Mistborn Book 1")
    assert not looks_like_series("Rob Jones")
    assert not looks_like_series("J. R. R. Tolkien")


def test_duration_verdict():
    assert duration_verdict(600, 602) == "match"
    assert duration_verdict(600, 300) == "mismatch"   # abridged
    assert duration_verdict(600, 650) == ""            # close-ish: no say either way
    assert duration_verdict(0, 600) == ""


def test_length_decides_between_editions():
    books = [
        {"title": "Dune", "author": "Frank Herbert", "asin": "ABRIDGED", "duration": 300},
        {"title": "Dune", "author": "Frank Herbert", "asin": "FULL", "duration": 1260},
    ]
    ranked = rank_candidates(books, "Dune", "Frank Herbert", audio_minutes=1255)
    assert ranked[0]["asin"] == "FULL" and ranked[0]["duration_match"]
    abridged = next(c for c in ranked if c["asin"] == "ABRIDGED")
    assert abridged["duration_mismatch"] and abridged["confidence"] <= 70


def test_same_edition_from_two_searches_is_listed_once():
    book = {"title": "Dune", "author": "Frank Herbert", "asin": "B1"}
    assert len(rank_candidates([book, dict(book)], "Dune", "Frank Herbert")) == 1
