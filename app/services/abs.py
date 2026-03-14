import logging
import requests

logger = logging.getLogger(__name__)


class AbsClient:
    def __init__(self, url: str, token: str):
        self.url = url.rstrip("/")
        self.headers = {"Authorization": f"Bearer {token}"}

    def _get(self, path: str, **kwargs):
        return requests.get(f"{self.url}{path}", headers=self.headers, timeout=15, **kwargs)

    def _post(self, path: str, **kwargs):
        return requests.post(f"{self.url}{path}", headers=self.headers, timeout=30, **kwargs)

    def get_libraries(self) -> list:
        resp = self._get("/api/libraries")
        resp.raise_for_status()
        return resp.json().get("libraries", [])

    def scan_library(self, library_id: str):
        """Trigger a library scan (returns immediately; scan runs async in ABS)."""
        resp = self._post(f"/api/libraries/{library_id}/scan")
        resp.raise_for_status()

    def search_library(self, library_id: str, query: str) -> list:
        """Search a library; returns list of matching libraryItem dicts."""
        resp = self._get(
            f"/api/libraries/{library_id}/search",
            params={"q": query, "limit": 10},
        )
        resp.raise_for_status()
        # ABS returns { book: [{libraryItem, matchKey, matchText}, ...], ... }
        results = resp.json().get("book", [])
        return [r["libraryItem"] for r in results if "libraryItem" in r]

    def search_books(self, title: str, author: str = "", provider: str = "audible") -> list:
        """Search for book metadata via ABS without modifying any library item."""
        params = {"title": title, "provider": provider}
        if author:
            params["author"] = author
        resp = self._get("/api/search/books", params=params)
        resp.raise_for_status()
        data = resp.json()
        return data if isinstance(data, list) else []

    def quick_match(self, item_id: str, title: str = "", author: str = "", provider: str = "audible") -> dict:
        """Trigger Quick Match on a library item to fill cover and metadata."""
        body = {"provider": provider}
        if title:
            body["title"] = title
        if author:
            body["author"] = author
        resp = self._post(f"/api/items/{item_id}/match", json=body)
        resp.raise_for_status()
        result = resp.json()
        logger.info(
            "AbsClient.quick_match: item=%s provider=%s updated=%s",
            item_id, provider, result.get("updated"),
        )
        return result
