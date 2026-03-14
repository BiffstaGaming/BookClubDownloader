import re
import hashlib
import logging
from urllib.parse import urljoin
import requests
from bs4 import BeautifulSoup

logger = logging.getLogger(__name__)


class AbookScraper:
    def __init__(self, base_url: str, username: str, password: str):
        self.base_url = base_url.rstrip("/")
        self.username = username
        self.password = password
        self._logged_in = False
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/120.0.0.0 Safari/537.36"
            )
        })

    def login(self) -> bool:
        """
        SMF login with client-side password hashing.
        1. GET the login page to extract hidden CSRF token fields.
        2. Compute hash_passwrd = SHA1(SHA1(password) + token_value[:32])
        3. POST with empty passwrd and the hashed value.
        """
        try:
            # Step 1: fetch login page to get hidden token fields
            login_page = self.session.get(
                f"{self.base_url}?action=login", timeout=30, allow_redirects=True
            )
            login_page.raise_for_status()
            soup = BeautifulSoup(login_page.text, "lxml")

            # Find the login form
            form = soup.find("form", id=re.compile(r"login", re.I)) or \
                   soup.find("form", action=re.compile(r"login2", re.I))

            # Collect all hidden fields (CSRF tokens etc.)
            hidden_fields = {}
            if form:
                for inp in form.find_all("input", type="hidden"):
                    name = inp.get("name")
                    value = inp.get("value", "")
                    if name:
                        hidden_fields[name] = value

            # Extract session token from the onsubmit handler:
            # onsubmit="hashLoginPassword(this, 'TOKEN')"
            token_value = ""
            onsubmit_match = re.search(
                r"hashLoginPassword\(this,\s*'([0-9a-f]{32})'", login_page.text
            )
            if onsubmit_match:
                token_value = onsubmit_match.group(1)
            else:
                # Fallback: look for a 32-char hex value among hidden fields
                for val in hidden_fields.values():
                    if re.fullmatch(r"[0-9a-f]{32}", val):
                        token_value = val
                        break

            # Step 2: compute SMF's client-side hash
            # Formula from sha1.js hashLoginPassword():
            # hash_passwrd = SHA1( SHA1(username.lower() + password) + session_token )
            if token_value:
                inner = hashlib.sha1(
                    (self.username.lower() + self.password).encode("utf-8")
                ).hexdigest()
                hash_passwrd = hashlib.sha1(
                    (inner + token_value).encode("utf-8")
                ).hexdigest()
            else:
                logger.warning("AbookScraper: no session token found, login will likely fail")
                hash_passwrd = hashlib.sha1(self.password.encode("utf-8")).hexdigest()

            # Step 3: build and POST the login form
            data = {
                **hidden_fields,
                "user": self.username,
                "passwrd": "",           # intentionally empty — JS clears it
                "hash_passwrd": hash_passwrd,
                "cookieneverexp": "on",
            }

            response = self.session.post(
                f"{self.base_url}?action=login2",
                data=data,
                timeout=30,
                allow_redirects=True,
            )
            response.raise_for_status()

            # A logout link in the page means we're authenticated
            if "action=logout" in response.text or "logout" in response.text.lower():
                self._logged_in = True
                logger.info("AbookScraper: login successful for %s", self.username)
                return True

            if self.username.lower() in response.text.lower():
                self._logged_in = True
                logger.info("AbookScraper: login successful for %s", self.username)
                return True

            logger.warning("AbookScraper: login failed — no logout link or username in response")
            self._logged_in = False
            return False

        except requests.RequestException as exc:
            logger.error("AbookScraper: login request failed: %s", exc)
            self._logged_in = False
            return False

    def ensure_logged_in(self):
        """Login if not already logged in."""
        if not self._logged_in:
            self.login()

    def search(self, query: str) -> list[dict]:
        """Search the forum and return a deduplicated list of topic dicts."""
        self.ensure_logged_in()
        try:
            response = self.session.post(
                f"{self.base_url}?action=search2",
                data={
                    "search": query,
                    "searchtype": "1",
                    "subject_only": "1",
                    "nograve": "1",
                },
                timeout=30,
                allow_redirects=True,
            )
            response.raise_for_status()
            soup = BeautifulSoup(response.text, "lxml")

            results = []
            seen_ids = set()

            # Results are in <h5> tags: Board / <a href="...topic=ID.msg...">Title</a>
            for h5 in soup.find_all("h5"):
                anchors = h5.find_all("a", href=True)
                # The last anchor in the h5 is the topic link
                topic_anchor = None
                for a in anchors:
                    if "topic=" in a.get("href", ""):
                        topic_anchor = a
                if not topic_anchor:
                    continue
                href = topic_anchor["href"]
                # topic=114952.msg133684 — extract only the numeric topic ID
                match = re.search(r"topic=(\d+)", href)
                if not match:
                    continue
                topic_id = match.group(1)
                if topic_id in seen_ids:
                    continue
                seen_ids.add(topic_id)
                title = topic_anchor.get_text(strip=True)
                if not title:
                    continue
                results.append({
                    "topic_id": topic_id,
                    "title": title,
                    "url": href if href.startswith("http") else urljoin(self.base_url + "/", href),
                })

            logger.info("AbookScraper.search: found %d results for query %r", len(results), query)
            return results
        except requests.RequestException as exc:
            logger.error("AbookScraper.search failed: %s", exc)
            raise

    def get_topic(self, topic_id: str) -> dict:
        """Fetch a topic page and return topic metadata with a list of posts."""
        self.ensure_logged_in()
        try:
            topic_url = f"{self.base_url}?topic={topic_id}"
            response = self.session.get(topic_url, timeout=30, allow_redirects=True)
            response.raise_for_status()
            soup = BeautifulSoup(response.text, "lxml")

            title_tag = soup.find("title")
            page_title = title_tag.get_text(strip=True) if title_tag else f"Topic {topic_id}"
            page_title = re.sub(r"\s*-\s*Book Club\s*$", "", page_title).strip()

            posts = []
            # Each post is wrapped in <div class="post_wrapper">
            for wrapper in soup.find_all("div", class_="post_wrapper"):
                # The post content div is <div class="inner" id="msg_NNNNNN">
                inner = wrapper.find("div", class_="inner")
                if not inner:
                    continue
                raw_id = inner.get("id", "")
                if not re.match(r"^msg_\d+$", raw_id):
                    continue
                msg_id = raw_id.replace("msg_", "")

                # Author is in <div class="poster"><h4><a>Name</a></h4>
                author = ""
                poster_div = wrapper.find("div", class_="poster")
                if poster_div:
                    h4 = poster_div.find("h4")
                    if h4:
                        author = h4.get_text(strip=True)

                # Subject line is in <h5 id="subject_MSGID">
                subject_el = wrapper.find("h5", id=f"subject_{msg_id}")
                subject = subject_el.get_text(strip=True) if subject_el else ""

                # Thank button is in <a class="thank_you_button_link"> inside .quickbuttons
                thank_anchor = wrapper.find("a", class_="thank_you_button_link")
                thank_href = thank_anchor["href"] if thank_anchor else ""

                # Check for hidden/revealed content inside the inner post div
                hiddenbox = inner.find("div", class_="hiddenbox")
                unhiddenbox = inner.find("div", class_="unhiddenbox")

                revealed = None
                if unhiddenbox:
                    revealed = self._parse_unhiddenbox(unhiddenbox)

                # Only include posts that have hidden or revealed content
                if not hiddenbox and not unhiddenbox:
                    continue

                posts.append({
                    "msg_id": msg_id,
                    "subject": subject,
                    "author": author,
                    "thank_href": thank_href,
                    "has_hidden": bool(hiddenbox),
                    "revealed": revealed,
                })

            logger.info("AbookScraper.get_topic: topic %s — %d relevant posts", topic_id, len(posts))
            return {"topic_id": topic_id, "title": page_title, "posts": posts}
        except requests.RequestException as exc:
            logger.error("AbookScraper.get_topic failed: %s", exc)
            raise

    def thank_and_get_content(self, topic_id: str, msg_id: str, thank_href: str) -> dict:
        """
        Thank a post (refresh=1 redirects back to topic with content revealed).
        Parse the unhiddenbox directly from the redirect response.
        """
        self.ensure_logged_in()
        try:
            full_thank_url = thank_href if thank_href.startswith("http") else urljoin(self.base_url + "/", thank_href)

            # refresh=1 means the server redirects back to the topic after thanking
            response = self.session.get(full_thank_url, timeout=30, allow_redirects=True)
            response.raise_for_status()
            soup = BeautifulSoup(response.text, "lxml")

            inner = soup.find("div", id=f"msg_{msg_id}")
            if not inner:
                return {"search_term": "", "password": "", "raw_text": "Post not found after thanking."}

            unhiddenbox = inner.find("div", class_="unhiddenbox")
            if unhiddenbox:
                result = self._parse_unhiddenbox(unhiddenbox)
                result["raw_text"] = inner.get_text(separator=" ", strip=True)
                return result

            return {"search_term": "", "password": "", "raw_text": raw_text}
        except requests.RequestException as exc:
            logger.error("AbookScraper.thank_and_get_content failed: %s", exc)
            raise

    # ------------------------------------------------------------------
    # Private helpers
    # ------------------------------------------------------------------

    def _parse_unhiddenbox(self, unhiddenbox) -> dict:
        """
        Extract search_term, password, and structured book metadata from
        an unhiddenbox div.

        The box contains two <code class="bbc_code"> tags:
          1st: search term, e.g. "PW - RJ-THF06-VTNCSP"
          2nd: password,    e.g. "Per.Ardua.Ad.Astra"

        The rest of the text contains structured fields like:
          Title:           The Vatican Conspiracy
          Author:          Rob Jones
          Series Name:     The Hunter Files
          Series Position: 06
        """
        raw_debug = unhiddenbox.get_text(separator="\n")
        logger.debug("_parse_unhiddenbox raw text:\n%s", raw_debug)

        codes = unhiddenbox.find_all("code", class_="bbc_code")
        search_term = codes[0].get_text(strip=True) if len(codes) > 0 else ""
        password = codes[1].get_text(strip=True) if len(codes) > 1 else ""
        # Strip leading "PW - " prefix from search term
        search_term = re.sub(r"^PW\s*-\s*", "", search_term).strip()

        # Extract structured metadata by parsing key: value lines
        raw = unhiddenbox.get_text(separator="\n")
        fields = {}
        for line in raw.splitlines():
            line = line.strip()
            if ":" in line:
                key, _, value = line.partition(":")
                key = key.strip().lower()
                value = value.strip()
                if key and value:
                    fields[key] = value

        title       = fields.get("title", "")
        author      = fields.get("author", "")
        series      = fields.get("series name", "")
        series_part = fields.get("series position", "")
        narrator    = fields.get("read by", "")

        # Normalise series_part: strip leading zeros but keep "1", "06" → "6"
        if series_part:
            try:
                series_part = str(int(series_part))
            except ValueError:
                pass  # leave as-is if it's not a plain number

        logger.info(
            "_parse_unhiddenbox: search_term=%r title=%r author=%r series=%r part=%r",
            search_term, title, author, series, series_part,
        )
        return {
            "search_term": search_term,
            "password":    password,
            "title":       title,
            "author":      author,
            "series":      series,
            "series_part": series_part,
            "narrator":    narrator,
        }
