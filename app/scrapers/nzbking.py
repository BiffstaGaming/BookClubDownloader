import re
import logging
import requests
from bs4 import BeautifulSoup

logger = logging.getLogger(__name__)

NZBKING_BASE = "https://www.nzbking.com"


class NzbkingScraper:
    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/120.0.0.0 Safari/537.36"
            )
        })

    def search(self, query: str) -> list[dict]:
        """
        Search nzbking.com and return a list of NZB result dicts.

        Page structure (div-based):
          <div class='search-result'>
            <div class='search-select'><input type="checkbox" name="nzb" value="HASH"></div>
            <div class='search-subject'>TITLE\n<a href="/nzb:HASH/">NZB</a> ... size: 258MB</div>
            <div class='search-age'>26d</div>
          </div>
        """
        try:
            response = self.session.get(
                f"{NZBKING_BASE}/search/",
                params={"q": query},
                timeout=30,
                allow_redirects=True,
            )
            response.raise_for_status()
            soup = BeautifulSoup(response.text, "lxml")

            results = []

            for result_div in soup.find_all("div", class_="search-result"):
                # Hash from checkbox value
                checkbox = result_div.find("input", attrs={"name": "nzb"})
                if not checkbox:
                    continue
                nzb_hash = checkbox.get("value", "").strip()
                if not nzb_hash:
                    continue

                # Title: first text node in search-subject (before any <br> or <a>)
                subject_div = result_div.find("div", class_="search-subject")
                title = ""
                size = ""
                if subject_div:
                    # Get the raw text of the div, first line is the title
                    raw = subject_div.get_text(separator="\n")
                    lines = [l.strip() for l in raw.splitlines() if l.strip()]
                    if lines:
                        title = lines[0]
                    # Size is embedded as "size: 258MB"
                    size_match = re.search(r"size:\s*(\S+)", raw, re.I)
                    if size_match:
                        size = size_match.group(1)

                # Age from search-age div
                age = ""
                age_div = result_div.find("div", class_="search-age")
                if age_div:
                    age = age_div.get_text(strip=True)

                results.append({
                    "nzb_hash": nzb_hash,
                    "title": title,
                    "size": size,
                    "age": age,
                    "detail_url": f"{NZBKING_BASE}/details:{nzb_hash}/",
                })

            logger.info("NzbkingScraper.search: found %d results for query %r", len(results), query)
            return results
        except requests.RequestException as exc:
            logger.error("NzbkingScraper.search failed: %s", exc)
            raise

    def download_nzb(self, nzb_hash: str) -> bytes:
        """Download an NZB file by its hash and return the raw bytes."""
        try:
            nzb_url = f"{NZBKING_BASE}/nzb:{nzb_hash}/"
            response = self.session.get(nzb_url, timeout=60, allow_redirects=True)
            response.raise_for_status()
            logger.info(
                "NzbkingScraper.download_nzb: downloaded %d bytes for hash %s",
                len(response.content), nzb_hash,
            )
            return response.content
        except requests.RequestException as exc:
            logger.error("NzbkingScraper.download_nzb failed for hash %s: %s", nzb_hash, exc)
            raise
