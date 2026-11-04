"""
qBittorrent (Web API v2) for torrent results from Prowlarr. BookClub adds the
torrent with a tag naming the download row (bookclub-<id>), then the background
poller follows it like an NZBGet job: progress, then the finished folder.

Torrent folders are never deleted after conversion — qBittorrent keeps seeding them.
"""
import logging

import requests

logger = logging.getLogger(__name__)

# States in which every byte is on disk (qBittorrent 5 says "stopped" where 4.x said "paused").
COMPLETE_STATES = {"uploading", "stalledUP", "pausedUP", "stoppedUP", "queuedUP", "forcedUP", "checkingUP"}
ERROR_STATES = {"error", "missingFiles"}


class QbitError(Exception):
    pass


def tag_for(download_id: int) -> str:
    return f"bookclub-{download_id}"


class QbitClient:
    def __init__(self, url: str, username: str = "", password: str = ""):
        self.url = (url or "").rstrip("/")
        self.username = username or ""
        self.password = password or ""
        self.session = requests.Session()
        # qBittorrent's CSRF check compares these with its own address.
        self.session.headers.update({"Referer": self.url, "Origin": self.url})
        self.logged_in = False

    def _login(self):
        resp = self.session.post(f"{self.url}/api/v2/auth/login",
                                 data={"username": self.username, "password": self.password}, timeout=15)
        if resp.status_code == 403:
            raise QbitError("qBittorrent has blocked this address after failed logins — wait, or check the password")
        resp.raise_for_status()
        if resp.text.strip() != "Ok.":
            raise QbitError("qBittorrent refused the username or password")
        self.logged_in = True

    def _request(self, method: str, path: str, **kwargs) -> requests.Response:
        for attempt in (1, 2):
            if self.username and not self.logged_in:
                self._login()
            resp = self.session.request(method, f"{self.url}{path}", timeout=30, **kwargs)
            if resp.status_code == 403 and attempt == 1 and self.username:
                self.logged_in = False  # session expired: log in again
                continue
            if resp.status_code == 403:
                raise QbitError("qBittorrent needs a username and password (or allow this address in its Web UI settings)")
            resp.raise_for_status()
            return resp
        raise QbitError("qBittorrent did not accept the login")

    def version(self) -> str:
        return self._request("GET", "/api/v2/app/version").text.strip()

    def add(self, tag: str, urls: str = "", torrent: bytes | None = None, category: str = "") -> None:
        """Add by magnet/URL or .torrent file. Always in its own folder, even a single-file torrent."""
        fields = {"tags": (None, tag), "contentLayout": (None, "Subfolder"), "root_folder": (None, "true")}
        if category:
            fields["category"] = (None, category)
        if urls:
            fields["urls"] = (None, urls)
        if torrent:
            fields["torrents"] = ("book.torrent", torrent, "application/x-bittorrent")
        resp = self._request("POST", "/api/v2/torrents/add", files=fields)
        answer = resp.text.strip()
        if answer and answer != "Ok.":
            raise QbitError(f"qBittorrent did not add the torrent ({answer[:80]})")

    def torrents(self, tag: str = "", hashes: list[str] | None = None) -> list[dict]:
        params = {}
        if tag:
            params["tag"] = tag
        if hashes:
            params["hashes"] = "|".join(hashes)
        data = self._request("GET", "/api/v2/torrents/info", params=params).json()
        return data if isinstance(data, list) else []


def torrent_state(info: dict) -> tuple[str, int]:
    """("downloading" | "downloaded" | "failed", progress 0-100) for a torrents/info entry."""
    state = info.get("state") or ""
    progress = int(round(float(info.get("progress") or 0) * 100))
    if state in ERROR_STATES:
        return "failed", progress
    if state in COMPLETE_STATES or progress >= 100:
        return "downloaded", 100
    return "downloading", min(progress, 99)


def fetch_torrent(url: str) -> tuple[str, bytes | None]:
    """
    A Prowlarr download link gives either a .torrent file or a redirect to a magnet
    link. Returns (magnet, None) or ("", torrent bytes).
    """
    resp = requests.get(url, timeout=60, allow_redirects=False)
    location = resp.headers.get("location", "")
    if resp.is_redirect and location.startswith("magnet:"):
        return location, None
    if resp.is_redirect and location:
        resp = requests.get(location, timeout=60)
    resp.raise_for_status()
    if not resp.content.startswith(b"d"):  # bencoded .torrent files are dictionaries
        raise QbitError("The indexer did not return a torrent file")
    return "", resp.content
