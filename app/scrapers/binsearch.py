import logging
import requests
from bs4 import BeautifulSoup

logger = logging.getLogger(__name__)

BINSEARCH_BASE = "https://www.binsearch.info"


class BinsearchScraper:
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
        Search www.binsearch.info and return a list of NZB result dicts.

        Page structure:
          <table class="... result-table">
            <tr>
              <td>N.</td>
              <td><input type="checkbox" class="mx-2" name="[base64-id]"/></td>
              <td>
                <a class="... font-medium ..." href="/details/[id]">Title</a>
                <a href="/search?poster=...">Poster</a>
              </td>
              <td class="min-w-20">9 days</td>
            </tr>
          </table>
        NZB download: GET /nzb?[base64-id]=1
        """
        try:
            response = self.session.get(
                BINSEARCH_BASE + "/search",
                params={"q": query},
                timeout=30,
                allow_redirects=True,
            )
            response.raise_for_status()
            soup = BeautifulSoup(response.text, "lxml")

            results = []

            table = soup.find("table", class_="result-table")
            if not table:
                logger.info("BinsearchScraper.search: no results table found for query %r", query)
                return results

            for row in table.find_all("tr"):
                # Checkbox holds the base64 NZB identifier as its name attribute
                checkbox = row.find("input", attrs={"type": "checkbox"})
                if not checkbox:
                    continue
                nzb_hash = checkbox.get("name", "").strip()
                if not nzb_hash:
                    continue

                # Title: the <a> with font-medium class (the subject link)
                title = ""
                detail_url = ""
                title_link = row.find("a", class_=lambda c: c and "font-medium" in c)
                if title_link:
                    title = title_link.get_text(separator=" ", strip=True)
                    href = title_link.get("href", "")
                    if href:
                        detail_url = BINSEARCH_BASE + href

                # Size: first <span class="...bg-white"> in the metadata badge row
                size = ""
                size_span = row.find("span", class_=lambda c: c and "bg-white" in c)
                if size_span:
                    size = size_span.get_text(strip=True)

                # Age: <td class="min-w-20">
                age = ""
                age_cell = row.find("td", class_=lambda c: c and "min-w-20" in c)
                if age_cell:
                    age = age_cell.get_text(strip=True)

                results.append({
                    "nzb_hash": nzb_hash,
                    "title": title or nzb_hash,
                    "size": size,
                    "age": age,
                    "detail_url": detail_url,
                    "source": "binsearch",
                })

            logger.info("BinsearchScraper.search: found %d results for query %r", len(results), query)
            return results
        except requests.RequestException as exc:
            logger.error("BinsearchScraper.search failed: %s", exc)
            raise

    def download_nzb(self, nzb_hash: str) -> bytes:
        """Download an NZB file via GET /nzb?{nzb_hash}=1."""
        try:
            response = self.session.get(
                BINSEARCH_BASE + "/nzb",
                params={nzb_hash: "1"},
                timeout=60,
                allow_redirects=True,
            )
            response.raise_for_status()
            logger.info(
                "BinsearchScraper.download_nzb: downloaded %d bytes for hash %s",
                len(response.content), nzb_hash,
            )
            return response.content
        except requests.RequestException as exc:
            logger.error("BinsearchScraper.download_nzb failed for hash %s: %s", nzb_hash, exc)
            raise
