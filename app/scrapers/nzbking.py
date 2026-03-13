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
        Each dict has: nzb_hash, title, size, age, detail_url
        """
        try:
            search_url = f"{NZBKING_BASE}/search/"
            params = {"q": query}
            response = self.session.get(search_url, params=params, timeout=30, allow_redirects=True)
            response.raise_for_status()
            soup = BeautifulSoup(response.text, "lxml")

            results = []

            # Try to find the nzblist form first
            nzblist_form = soup.find("form", attrs={"name": "nzblist"})
            search_root = nzblist_form if nzblist_form else soup

            # Find all checkbox inputs with name="nzb"
            checkboxes = search_root.find_all("input", attrs={"name": "nzb"})

            for checkbox in checkboxes:
                nzb_hash = checkbox.get("value", "").strip()

                # Try to find the hash from nearby links if not in value
                row = checkbox.find_parent("tr") or checkbox.find_parent("div")
                if not row:
                    continue

                if not nzb_hash:
                    # Look for /nzb:HASH/ or /details:HASH/ pattern in links
                    for link in row.find_all("a", href=True):
                        hash_match = re.search(r"/(?:nzb|details):([A-Za-z0-9]+)/", link["href"])
                        if hash_match:
                            nzb_hash = hash_match.group(1)
                            break

                if not nzb_hash:
                    continue

                # Extract title from the subject link
                title = ""
                detail_url = ""
                for link in row.find_all("a", href=True):
                    href = link["href"]
                    # Subject links typically point to /details:HASH/ or /nzb:HASH/
                    if re.search(r"/(?:details|nzb):", href):
                        title = link.get_text(strip=True)
                        if href.startswith("http"):
                            detail_url = href
                        else:
                            detail_url = NZBKING_BASE + href
                        break

                if not title:
                    # Grab the most descriptive text in the row
                    title = row.get_text(separator=" ", strip=True)[:120]

                # Extract size and age from table cells
                size = ""
                age = ""
                cells = row.find_all("td")
                # Heuristic: iterate cells looking for size/age patterns
                for cell in cells:
                    cell_text = cell.get_text(strip=True)
                    if re.search(r"\d+(\.\d+)?\s*(GB|MB|KB)", cell_text, re.I):
                        size = cell_text
                    elif re.search(r"\d+\s*(d|day|h|hour|w|week|y|year)", cell_text, re.I):
                        age = cell_text

                results.append({
                    "nzb_hash": nzb_hash,
                    "title": title,
                    "size": size,
                    "age": age,
                    "detail_url": detail_url,
                })

            logger.info(
                "NzbkingScraper.search: found %d results for query '%s'", len(results), query
            )
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
