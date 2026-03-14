import re
import logging
import requests
from bs4 import BeautifulSoup

logger = logging.getLogger(__name__)

BINSEARCH_BASE = "https://binsearch.info"


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
        Search binsearch.net and return a list of NZB result dicts.

        Page structure (table-based):
          <table id="r2">
            <tr class="even">
              <td><input type="checkbox" name="MSGID" value="1" /></td>
              <td class="subject"><span id="sN">Title <b>"file.rar"</b> <span class="d">poster</span></span></td>
              <td>01-Jan-2024</td>
              <td class="right">252.0 MB</td>
              <td>group</td>
            </tr>
          </table>
        """
        try:
            response = self.session.get(
                BINSEARCH_BASE + "/",
                params={"q": query, "max": "100", "adv_age": "", "server": ""},
                timeout=30,
                allow_redirects=True,
            )
            response.raise_for_status()
            soup = BeautifulSoup(response.text, "lxml")

            results = []

            table = soup.find("table", id="r2")
            if not table:
                logger.info("BinsearchScraper.search: no results table found for query %r", query)
                return results

            for row in table.find_all("tr", class_=["even", "odd"]):
                # Message ID from checkbox name
                checkbox = row.find("input", attrs={"type": "checkbox"})
                if not checkbox:
                    continue
                nzb_hash = checkbox.get("name", "").strip()
                if not nzb_hash:
                    continue

                cells = row.find_all("td")
                if len(cells) < 3:
                    continue

                # Title: text from subject cell, stripping poster and filename spans
                title = ""
                subject_cell = row.find("td", class_="subject") or (cells[1] if len(cells) > 1 else None)
                if subject_cell:
                    # Remove the poster <span class="d"> so it doesn't pollute the title
                    for d_span in subject_cell.find_all("span", class_="d"):
                        d_span.decompose()
                    raw = subject_cell.get_text(separator=" ", strip=True)
                    # Strip quoted filenames like "file.part01.rar"
                    raw = re.sub(r'"[^"]*"', "", raw).strip()
                    title = raw or nzb_hash

                # Date — typically the 3rd cell (index 2)
                age = ""
                if len(cells) > 2:
                    age = cells[2].get_text(strip=True)

                # Size — typically the 4th cell (index 3)
                size = ""
                if len(cells) > 3:
                    size = cells[3].get_text(strip=True)

                results.append({
                    "nzb_hash": nzb_hash,
                    "title": title,
                    "size": size,
                    "age": age,
                    "detail_url": "",
                    "source": "binsearch",
                })

            logger.info("BinsearchScraper.search: found %d results for query %r", len(results), query)
            return results
        except requests.RequestException as exc:
            logger.error("BinsearchScraper.search failed: %s", exc)
            raise

    def download_nzb(self, nzb_hash: str) -> bytes:
        """Download an NZB file by POSTing the message ID to binsearch."""
        try:
            response = self.session.post(
                BINSEARCH_BASE + "/",
                data={"action": "nzb", nzb_hash: "1"},
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
