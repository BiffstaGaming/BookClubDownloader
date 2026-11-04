"""
The page libraries (Bootstrap, Bootstrap Icons, htmx) are served from static/vendor,
so BookClub's pages work without the internet. A file with a wrong `integrity`
hash is silently refused by the browser — which is how Bootstrap's JavaScript once
went missing unnoticed — so every declared hash is checked against the file.
"""
import base64
import hashlib
import re
from pathlib import Path

TAG = re.compile(r"<(?:script|link)\b[^>]*>", re.S)
ATTR = re.compile(r'(src|href|integrity)="([^"]+)"')


def _tags():
    for template in Path("app/templates").rglob("*.html"):
        for tag in TAG.findall(template.read_text(encoding="utf-8")):
            yield template, dict(ATTR.findall(tag))


def test_no_page_loads_libraries_from_the_internet():
    remote = [(str(t), a.get("src") or a.get("href")) for t, a in _tags()
              if (a.get("src") or a.get("href") or "").startswith(("http://", "https://", "//"))]
    assert remote == []


def test_every_integrity_hash_matches_the_local_file():
    checked = 0
    for template, attrs in _tags():
        if "integrity" not in attrs:
            continue
        url = attrs.get("src") or attrs.get("href")
        assert url.startswith("/static/"), url
        algorithm, _, expected = attrs["integrity"].partition("-")
        body = Path(url.lstrip("/").split("?")[0]).read_bytes()
        assert base64.b64encode(hashlib.new(algorithm, body).digest()).decode() == expected, f"{template}: {url}"
        checked += 1
    assert checked >= 6  # bootstrap css + js and htmx, in both base templates


def test_local_libraries_are_served(client):
    for path in ["/static/vendor/bootstrap/bootstrap.min.css", "/static/vendor/bootstrap/bootstrap.bundle.min.js",
                 "/static/vendor/htmx/htmx.min.js", "/static/vendor/bootstrap-icons/bootstrap-icons.min.css",
                 "/static/vendor/bootstrap-icons/fonts/bootstrap-icons.woff2",
                 "/static/vendor/bootstrap-icons/fonts/bootstrap-icons.woff"]:
        assert client.get(path).status_code == 200, path
