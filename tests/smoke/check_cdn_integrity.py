"""
CI check: every CDN file the templates load with an `integrity` hash really has
that hash. A wrong hash makes the browser silently refuse the file — that is how
Bootstrap's JavaScript (mobile menu, alert close buttons) went missing unnoticed.

Run from the repo root: python tests/smoke/check_cdn_integrity.py
"""
import base64
import hashlib
import re
import sys
import urllib.request
from pathlib import Path

TAG = re.compile(r"<(?:script|link)\b[^>]*>", re.S)
ATTR = re.compile(r'(src|href|integrity)="([^"]+)"')

failures, checked = [], 0
for template in Path("app/templates").rglob("*.html"):
    for tag in TAG.findall(template.read_text(encoding="utf-8")):
        attrs = dict(ATTR.findall(tag))
        if "integrity" not in attrs:
            continue
        url = attrs.get("src") or attrs.get("href")
        algorithm, _, expected = attrs["integrity"].partition("-")
        body = urllib.request.urlopen(url, timeout=30).read()
        actual = base64.b64encode(hashlib.new(algorithm, body).digest()).decode()
        checked += 1
        status = "OK  " if actual == expected else "FAIL"
        print(f"{status} {template}: {url}")
        if actual != expected:
            print(f"     declared {algorithm}-{expected}\n     actual   {algorithm}-{actual}")
            failures.append(url)

print(f"{checked} file(s) checked")
sys.exit(1 if failures or not checked else 0)
