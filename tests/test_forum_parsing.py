from bs4 import BeautifulSoup

from app.scrapers.abook import AbookScraper


def box(inner_html: str):
    """An SMF 'unhiddenbox' with the structure abook.link uses (label, code header, code)."""
    html = f'<div class="unhiddenbox">Hidden content:<br />{inner_html}</div>'
    return BeautifulSoup(html, "lxml").find("div", class_="unhiddenbox")


def code(label: str, value: str) -> str:
    return (f'{label}<br /><div class="codeheader"><span class="code">Code:</span> '
            f'<a class="codeoperation">[Copy]</a></div><code class="bbc_code">{value}</code><br />')


def parse(inner_html: str) -> dict:
    return AbookScraper("https://example/index.php", "u", "p")._parse_unhiddenbox(box(inner_html))


def test_m4b_mp3_epub_post_uses_password_box_and_m4b_search():
    # The Stand: four boxes; the 2nd is the MP3 search term, not the password
    result = parse(code("M4B Search:", "abook.link - 10f1e4d6dde2a8f128d1")
                   + code("MP3 Search:", "abook.link - 999c0af749c07d77d43a")
                   + code("EPUB Search:", "abook.link - 376bfd840e8d3d56f2cd")
                   + code("Password:", "oral-massif-footage-eighty"))
    assert result["password"] == "oral-massif-footage-eighty"
    assert result["search_term"] == "abook.link - 10f1e4d6dde2a8f128d1"


def test_mp3_only_post_uses_mp3_search():
    result = parse(code("MP3 Search:", "abook.link - mp3term") + code("Password:", "pw1"))
    assert (result["search_term"], result["password"]) == ("abook.link - mp3term", "pw1")


def test_plain_search_and_password_post():
    # It: "Search:" + "Password:"
    result = parse(code("Search:", "7lfgPtdRjvP1vOstWwg07E") + code("Password:", "cogito.ergo.sum"))
    assert (result["search_term"], result["password"]) == ("7lfgPtdRjvP1vOstWwg07E", "cogito.ergo.sum")


def test_password_listed_first():
    result = parse(code("Password:", "first-pw") + code("Search:", "term-after"))
    assert (result["search_term"], result["password"]) == ("term-after", "first-pw")


def test_unlabelled_boxes_fall_back_to_order():
    result = parse('<code class="bbc_code">PW - RJ-THF06-VTNCSP</code><code class="bbc_code">Per.Ardua.Ad.Astra</code>')
    assert (result["search_term"], result["password"]) == ("RJ-THF06-VTNCSP", "Per.Ardua.Ad.Astra")


def test_no_password_box():
    result = parse(code("Search:", "only-a-term"))
    assert (result["search_term"], result["password"]) == ("only-a-term", "")


class _Response:
    def __init__(self, text):
        self.text = text

    def raise_for_status(self):
        pass


def test_search_titles_keep_spaces_around_highlighted_words():
    # Search results wrap each searched word in <strong class="highlight">
    hl = '<strong class="highlight">{}</strong>'.format
    html = (f'<h5><a href="?board=5.0">Romance</a> / <a href="?topic=123.msg456#msg456">Rina Kent - '
            f'{hl("Legacy")} {hl("of")} {hl("Gods")} 01 - God {hl("of")} Malice (2022)</a></h5>'
            f'<h5><a href="?board=7.0">Fantasy</a> / <a href="?topic=124.msg457#msg457">Stacey Brutger - '
            f'Clash {hl("of")} the Demi{hl("gods")} 03 - Daemon {hl("Legacy")} (2022)</a></h5>')
    scraper = AbookScraper("https://example/index.php", "u", "p")
    scraper._request = lambda *args, **kwargs: _Response(html)
    titles = [t["title"] for t in scraper.search("legacy of gods")]
    assert titles == ["Rina Kent - Legacy of Gods 01 - God of Malice (2022)",
                      "Stacey Brutger - Clash of the Demigods 03 - Daemon Legacy (2022)"]


def test_series_watch_reads_highlighted_search_titles():
    from app.services.wishlist import by_author, parse_series_topic, same_series
    title = "Rina Kent - Legacy of Gods 01 - God of Malice (2022)"
    series, number = parse_series_topic(title)
    assert (series, number) == ("Legacy of Gods", "1")
    assert same_series("Legacy of Gods", series) and by_author("Rina Kent", title)
