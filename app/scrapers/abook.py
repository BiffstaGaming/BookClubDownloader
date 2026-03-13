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

            # Page title
            title_tag = soup.find("title")
            page_title = title_tag.get_text(strip=True) if title_tag else f"Topic {topic_id}"
            # Clean up common SMF title suffixes
            page_title = re.sub(r"\s*-\s*\S+$", "", page_title).strip()

            posts = []
            # Posts in SMF have ids like "msg_NNNNNN"
            for post_el in soup.find_all(id=re.compile(r"^msg_\d+")):
                raw_id = post_el.get("id", "")
                msg_id = raw_id.replace("msg_", "")

                # Find author
                author = ""
                author_el = post_el.find(class_=re.compile(r"poster|author|name", re.I))
                if author_el:
                    author = author_el.get_text(strip=True)
                else:
                    # Fallback: look for a link near the post header
                    poster_link = post_el.find("a", href=re.compile(r"action=profile"))
                    if poster_link:
                        author = poster_link.get_text(strip=True)

                # Find thank link
                thank_href = ""
                thank_anchor = post_el.find("a", href=re.compile(r"action=thank"))
                if not thank_anchor:
                    # Sometimes the thank link is outside the post div — search near siblings
                    parent = post_el.parent
                    if parent:
                        thank_anchor = parent.find("a", href=re.compile(r"action=thank"))
                if thank_anchor:
                    thank_href = thank_anchor.get("href", "")

                has_hidden = bool(
                    post_el.find(class_=re.compile(r"hidden|spoiler|locked", re.I))
                    or post_el.find("div", class_=re.compile(r"bbc_spoiler|thankspost", re.I))
                )

                posts.append({
                    "msg_id": msg_id,
                    "thank_href": thank_href,
                    "author": author,
                    "has_hidden": has_hidden,
                })

            logger.info(
                "AbookScraper.get_topic: topic %s has %d posts", topic_id, len(posts)
            )
            return {
                "topic_id": topic_id,
                "title": page_title,
                "posts": posts,
            }
        except requests.RequestException as exc:
            logger.error("AbookScraper.get_topic failed: %s", exc)
            raise

    def thank_and_get_content(self, topic_id: str, msg_id: str, thank_href: str) -> dict:
        """
        Thank a post to reveal hidden content, then scrape the revealed text.
        Returns dict with search_term, password, raw_text.
        """
        self.ensure_logged_in()
        try:
            # Build the full thank URL
            if thank_href.startswith("http"):
                full_thank_url = thank_href
            else:
                # Relative URL — join with base_url
                full_thank_url = urljoin(self.base_url + "/", thank_href)

            # Step 1: perform the thank action
            self.session.get(full_thank_url, timeout=30, allow_redirects=True)

            # Step 2: re-fetch the post page to get revealed content
            post_url = f"{self.base_url}?topic={topic_id}.msg{msg_id}#msg{msg_id}"
            response = self.session.get(post_url, timeout=30, allow_redirects=True)
            response.raise_for_status()
            soup = BeautifulSoup(response.text, "lxml")

            # Step 3: find the specific post element
            post_el = soup.find(id=f"msg_{msg_id}")
            if not post_el:
                # Fallback: grab full page text
                raw_text = soup.get_text(separator=" ", strip=True)
            else:
                raw_text = post_el.get_text(separator=" ", strip=True)

            # Step 4: parse search_term
            search_term = self._extract_search_term(raw_text)

            # Step 5: parse password
            password = self._extract_password(raw_text)

            logger.info(
                "AbookScraper.thank_and_get_content: topic=%s msg=%s search_term=%r password=%r",
                topic_id, msg_id, search_term, password,
            )
            return {
                "search_term": search_term,
                "password": password,
                "raw_text": raw_text,
            }
        except requests.RequestException as exc:
            logger.error("AbookScraper.thank_and_get_content failed: %s", exc)
            raise

    # ------------------------------------------------------------------
    # Private helpers
    # ------------------------------------------------------------------

    def _extract_search_term(self, text: str) -> str:
        """Try multiple patterns to extract an NZB search term."""
        patterns = [
            r"[Ss]earch[:\s]+([A-Z0-9][A-Z0-9\-\.]{4,})",
            r"[Nn][Zz][Bb][:\s]+([A-Z0-9][A-Z0-9\-\.]{4,})",
            r"[Qq]uery[:\s]+([A-Z0-9][A-Z0-9\-\.]{4,})",
            r"[Tt]itle[:\s]+([A-Z0-9][A-Z0-9\-\.]{4,})",
        ]
        for pattern in patterns:
            match = re.search(pattern, text)
            if match:
                return match.group(1).strip()

        # Fallback: find standalone uppercase alphanumeric-dash strings of length > 8
        candidates = re.findall(r"\b([A-Z][A-Z0-9\-\.]{8,})\b", text)
        if candidates:
            # Return the longest candidate as it's most likely a proper search term
            return max(candidates, key=len)

        return ""

    def _extract_password(self, text: str) -> str:
        """Try multiple patterns to extract a password."""
        patterns = [
            r"[Pp]ass(?:word)?[:\s]+(\S+)",
            r"PW[:\s]+(\S+)",
            r"[Pp]w[:\s]+(\S+)",
            r"[Kk]ey[:\s]+(\S+)",
        ]
        for pattern in patterns:
            match = re.search(pattern, text)
            if match:
                candidate = match.group(1).strip().rstrip(".,;)")
                if candidate:
                    return candidate
        return ""
