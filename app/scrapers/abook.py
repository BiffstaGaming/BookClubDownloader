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

            # Find the session/form token — SMF uses a random field name whose
            # value is a 32-char hex string
            token_value = ""
            for val in hidden_fields.values():
                if re.fullmatch(r"[0-9a-f]{32}", val):
                    token_value = val
                    break

            # Step 2: compute SMF's client-side hash
            # hash_passwrd = SHA1( SHA1(password) + token_value[:32] )
            if token_value:
                inner = hashlib.sha1(self.password.encode("utf-8")).hexdigest()
                hash_passwrd = hashlib.sha1(
                    (inner + token_value[:32]).encode("utf-8")
                ).hexdigest()
            else:
                # Fallback: some older SMF installs just use SHA1(password)
                logger.warning("AbookScraper: no token found, using plain SHA1 hash")
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
            # Step 1: GET the search form to extract hidden CSRF fields
            search_form_resp = self.session.get(
                f"{self.base_url}?action=search", timeout=30, allow_redirects=True
            )
            search_form_resp.raise_for_status()
            form_soup = BeautifulSoup(search_form_resp.text, "lxml")

            hidden_fields = {}
            search_form = form_soup.find("form", id=re.compile(r"search", re.I)) or \
                          form_soup.find("form", action=re.compile(r"search2", re.I)) or \
                          form_soup.find("form")
            if search_form:
                for inp in search_form.find_all("input", type="hidden"):
                    name = inp.get("name")
                    value = inp.get("value", "")
                    if name:
                        hidden_fields[name] = value
                # Detect the actual action URL from the form
                form_action = search_form.get("action", "")
                logger.info("AbookScraper.search: form action=%r hidden=%r", form_action, list(hidden_fields.keys()))
            else:
                logger.warning("AbookScraper.search: could not find search form on page")

            # Step 2: POST search — try the action from the form first, fall back to known URLs
            if form_action:
                if form_action.startswith("http"):
                    search_url = form_action
                else:
                    search_url = urljoin(self.base_url + "/", form_action)
            else:
                search_url = f"{self.base_url}?action=search2;sa=results"

            data = {
                **hidden_fields,
                "search": query,
                "searchtype": "1",
                "subjectonly": "1",
            }
            logger.info("AbookScraper.search: posting to %r with query=%r", search_url, query)
            response = self.session.post(search_url, data=data, timeout=30, allow_redirects=True)
            response.raise_for_status()

            # Log a snippet so we can see what came back
            logger.info(
                "AbookScraper.search: response url=%r status=%d snippet=%r",
                response.url, response.status_code, response.text[:500],
            )

            soup = BeautifulSoup(response.text, "lxml")

            results = []
            seen_ids = set()

            for anchor in soup.find_all("a", href=True):
                href = anchor["href"]
                if "topic=" not in href:
                    continue
                match = re.search(r"topic=(\d+)", href)
                if not match:
                    continue
                topic_id = match.group(1)
                if topic_id in seen_ids:
                    continue
                seen_ids.add(topic_id)
                title = anchor.get_text(strip=True)
                if not title:
                    continue
                if href.startswith("http"):
                    url = href
                else:
                    url = urljoin(self.base_url + "/", href)
                results.append({
                    "topic_id": topic_id,
                    "title": title,
                    "url": url,
                })

            logger.info("AbookScraper.search: found %d results for query '%r'", len(results), query)
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
