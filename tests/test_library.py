import json
import os

from app.services import library


def test_sanitize_keeps_non_ascii_letters_and_strips_forbidden_chars():
    assert library.sanitize_filename("Café: The “Story”?") == 'Cafe_ The _Story__'
    assert library.sanitize_filename("Война и мир") == "Война и мир"
    assert library.sanitize_filename("  ...  ") == "download"


def test_default_layout_series_book():
    meta = {"title": "The Way of Kings", "author": "Brandon Sanderson",
            "series": "The Stormlight Archive", "series_part": "1"}
    assert library.final_book_path("/m4b", "", meta) == \
        "/m4b/Brandon Sanderson/The Stormlight Archive/1 - The Way of Kings/The Way of Kings.m4b"


def test_default_layout_standalone_drops_empty_parts():
    meta = {"title": "Project Hail Mary", "author": "Andy Weir"}
    assert library.final_book_path("/m4b", "", meta) == "/m4b/Andy Weir/Project Hail Mary/Project Hail Mary.m4b"


def test_series_without_number():
    meta = {"title": "T", "author": "A", "series": "S"}
    assert library.final_book_path("/m4b", "", meta) == "/m4b/A/S/T/T.m4b"


def test_legacy_template_with_filename_still_works():
    meta = {"title": "Dune", "author": "Frank Herbert"}
    assert library.final_book_path("/m4b", "/audiobooks/[Author]/[Filename]", meta) == \
        "/audiobooks/Frank Herbert/Dune.m4b"
    assert library.final_book_path("/m4b", "/audiobooks/[Author]/[Series]/[Filename]", meta) == \
        "/audiobooks/Frank Herbert/Dune.m4b"


def test_template_without_filename_appends_one():
    meta = {"title": "Dune", "author": "Frank Herbert", "narrator": "Scott Brick", "year": "2006"}
    assert library.final_book_path("/m4b", "/books/[Author]/[Year] - [Title] {[Narrator]}", meta) == \
        "/books/Frank Herbert/2006 - Dune {Scott Brick}/Dune.m4b"


def test_layout_warning():
    assert library.template_layout_warning("/audiobooks/[Author]/[Filename]")
    assert library.template_layout_warning("/audiobooks/[Author]/[Title].m4b")
    assert not library.template_layout_warning("/audiobooks/[Author]/[Title]/[Filename]")
    assert not library.template_layout_warning("/audiobooks/[Author]/[Series]/[BookNumber] - [Title]")
    assert not library.template_layout_warning("")


def test_abs_metadata_json_format():
    meta = {"title": "The Way of Kings", "author": "Brandon Sanderson", "narrator": "Michael Kramer, Kate Reading",
            "series": "The Stormlight Archive", "series_part": "1", "year": "2010", "asin": "B003ZWFO7E",
            "genres": ["Fantasy"], "description": "Roshar..."}
    out = library.build_abs_metadata(meta)
    assert out["authors"] == ["Brandon Sanderson"]
    assert out["narrators"] == ["Michael Kramer", "Kate Reading"]
    assert out["series"] == ["The Stormlight Archive #1"]
    assert out["publishedYear"] == "2010"
    assert out["asin"] == "B003ZWFO7E"
    assert out["genres"] == ["Fantasy"]


def test_write_abs_metadata(tmp_path):
    path = library.write_abs_metadata(str(tmp_path), {"title": "X", "author": "Y"})
    assert os.path.basename(path) == "metadata.json"
    assert json.loads(open(path, encoding="utf-8").read())["title"] == "X"


def test_safe_delete_only_inside_download_roots(tmp_path):
    downloads = tmp_path / "downloads"
    book = downloads / "completed" / "Some Book"
    book.mkdir(parents=True)
    (book / "a.mp3").write_bytes(b"x")
    library_dir = tmp_path / "library"
    library_dir.mkdir()

    # The downloads root itself is never deleted
    assert library.safe_delete_source(str(downloads), [str(downloads)], [])[0] is False
    # Folders outside the roots are never deleted
    assert library.safe_delete_source(str(library_dir), [str(downloads)], [])[0] is False
    # A folder that contains the library is never deleted
    assert library.safe_delete_source(str(downloads / "completed"), [str(downloads)],
                                      [str(downloads / "completed" / "Some Book")])[0] is False
    assert book.exists()
    deleted, _ = library.safe_delete_source(str(book), [str(downloads)], [str(library_dir)])
    assert deleted and not book.exists()


def test_map_path():
    assert library.map_path("/downloads/completed/x", "/downloads", "/data/dl") == "/data/dl/completed/x"
    assert library.map_path("/other/x", "/downloads", "/data/dl") == "/other/x"
    assert library.map_path("/downloads2/x", "/downloads", "/data/dl") == "/downloads2/x"
    assert library.map_path("/downloads/x", "", "") == "/downloads/x"


def test_find_audio_and_cover(tmp_path):
    (tmp_path / "CD2").mkdir()
    (tmp_path / "CD1").mkdir()
    (tmp_path / "CD2" / "01.flac").write_bytes(b"x")
    (tmp_path / "CD1" / "01.opus").write_bytes(b"x")
    (tmp_path / "info.nfo").write_bytes(b"x")
    (tmp_path / "back.jpg").write_bytes(b"x" * 10)
    (tmp_path / "Folder.JPG").write_bytes(b"x")
    files = [os.path.relpath(f, tmp_path).replace("\\", "/") for f in library.find_audio_files(str(tmp_path))]
    assert files == ["CD1/01.opus", "CD2/01.flac"]
    assert library.find_cover_image(str(tmp_path)).endswith("Folder.JPG")
