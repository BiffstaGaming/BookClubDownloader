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
