from app.services.matching import audible_fields, compute_confidence, parse_nzb_name, rank_candidates

AUDIBLE_RESULT = {
    "title": "The Vatican Conspiracy",
    "subtitle": "The Hunter Files, Book 6",
    "author": "Rob Jones",
    "narrator": "Colin Mace",
    "publisher": "Audible Studios",
    "publishedYear": "2019",
    "description": "<p>An ancient secret...</p>",
    "cover": "https://m.media-amazon.com/images/I/x.jpg",
    "asin": "B07XYZ",
    "isbn": None,
    "genres": ["Thriller"],
    "series": [{"series": "The Hunter Files", "sequence": "6"}],
    "language": "English",
    "duration": 545,
}


def test_parse_nzb_name():
    assert parse_nzb_name("Book Club - [SPOT] Rob Jones - Hunter06 - The Vatican Conspiracy (Colin Mace)") == \
        ("The Vatican Conspiracy", "Rob Jones")
    assert parse_nzb_name("Author - Title") == ("Title", "")
    assert parse_nzb_name("Just A Title") == ("Just A Title", "")


def test_confidence():
    assert compute_confidence("The Vatican Conspiracy", "Rob Jones", "The Vatican Conspiracy", "Rob Jones") == 100
    assert compute_confidence("Completely Different", "", "The Vatican Conspiracy", "Rob Jones") < 50
    # Multiple Audible authors: score against the best one
    assert compute_confidence("Good Omens", "Neil Gaiman", "Good Omens", "Terry Pratchett, Neil Gaiman") == 100


def test_audible_fields_from_abs_shape():
    f = audible_fields(AUDIBLE_RESULT)
    assert f["series"] == "The Hunter Files" and f["series_part"] == "6"
    assert f["narrator"] == "Colin Mace"
    assert f["description"] == "An ancient secret..."
    assert f["asin"] == "B07XYZ" and f["year"] == "2019"
    assert f["cover_url"].startswith("https://")


def test_audible_fields_handles_nulls():
    f = audible_fields({"title": "X", "series": None, "genres": None, "author": "A"})
    assert f["series"] == "" and f["genres"] == []


def test_rank_candidates_best_first():
    other = dict(AUDIBLE_RESULT, title="The Atlantis Covenant", asin="B0OTHER")
    ranked = rank_candidates([other, AUDIBLE_RESULT], "The Vatican Conspiracy", "Rob Jones")
    assert ranked[0]["asin"] == "B07XYZ"
    assert ranked[0]["confidence"] > ranked[1]["confidence"]


GERMAN_EDITION = dict(AUDIBLE_RESULT, title="The Poison Daughter", author="Sheila Masterson", asin="B0GERMAN",
                      language="german", narrator="Mona Fischer, Till Beck", publisher="der Hörverlag")
ENGLISH_EDITION = dict(AUDIBLE_RESULT, title="The Poison Daughter", author="Sheila Masterson", asin="B0ENGLISH",
                       language="English", narrator="Some English Narrator")


def test_foreign_edition_with_same_title_is_ranked_below_and_capped():
    ranked = rank_candidates([GERMAN_EDITION, ENGLISH_EDITION], "The Poison Daughter", "Sheila Masterson",
                             preferred_language="English")
    assert ranked[0]["asin"] == "B0ENGLISH" and ranked[0]["confidence"] == 100
    german = ranked[1]
    assert german["language_mismatch"] and german["confidence"] <= 60


def test_only_foreign_editions_never_reach_auto_convert_threshold():
    ranked = rank_candidates([GERMAN_EDITION], "The Poison Daughter", "Sheila Masterson", preferred_language="English")
    assert ranked[0]["confidence"] < 90


def test_any_language_and_unknown_language_are_not_penalised():
    assert rank_candidates([GERMAN_EDITION], "The Poison Daughter", "Sheila Masterson",
                           preferred_language="any")[0]["confidence"] == 100
    unknown = dict(GERMAN_EDITION, language="")
    assert rank_candidates([unknown], "The Poison Daughter", "Sheila Masterson",
                           preferred_language="English")[0]["confidence"] == 100


def test_language_codes_are_understood():
    from app.services.matching import language_mismatch
    assert not language_mismatch("en", "English")
    assert language_mismatch("de", "English")
