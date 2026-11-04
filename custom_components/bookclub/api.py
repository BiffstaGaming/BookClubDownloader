"""A small async client for the BookClub HTTP API."""

from __future__ import annotations

import asyncio
from typing import Any

import aiohttp

TIMEOUT = aiohttp.ClientTimeout(total=20)


class BookClubError(Exception):
    """Base error talking to BookClub."""


class BookClubConnectionError(BookClubError):
    """BookClub could not be reached (or answered with garbage)."""


class BookClubAuthError(BookClubError):
    """The API token was refused."""


class BookClubResponseError(BookClubError):
    """BookClub answered with an error, e.g. 404/409/422."""

    def __init__(self, status: int, detail: str) -> None:
        super().__init__(detail or f"HTTP {status}")
        self.status = status
        self.detail = detail


def normalize_url(url: str) -> str:
    """'bookclub:9933/' -> 'http://bookclub:9933'."""
    url = (url or "").strip()
    if url and "://" not in url:
        url = f"http://{url}"
    return url.rstrip("/")


def _detail(body: Any) -> str:
    if isinstance(body, dict):
        detail = body.get("detail")
        if isinstance(detail, str):
            return detail
        if isinstance(detail, list) and detail:  # FastAPI validation errors
            first = detail[0]
            if isinstance(first, dict):
                return str(first.get("msg", first))
            return str(first)
        if detail is not None:
            return str(detail)
    return ""


class BookClubApi:
    """Talks to one BookClub server."""

    def __init__(self, session: aiohttp.ClientSession, url: str, token: str) -> None:
        self._session = session
        self.url = normalize_url(url)
        self._token = (token or "").strip()

    async def _request(
        self, method: str, path: str, *, auth: bool = True, json: Any = None, params: dict | None = None
    ) -> Any:
        headers = {"Accept": "application/json"}
        if auth:
            headers["Authorization"] = f"Bearer {self._token}"
        try:
            async with self._session.request(
                method, f"{self.url}{path}", headers=headers, json=json, params=params, timeout=TIMEOUT
            ) as resp:
                try:
                    body = await resp.json(content_type=None)
                except ValueError:
                    body = None
                if resp.status == 401:
                    raise BookClubAuthError(_detail(body) or "The API token was refused")
                if resp.status >= 400:
                    raise BookClubResponseError(resp.status, _detail(body))
                if body is None and resp.status != 204:
                    raise BookClubConnectionError(f"{path} did not return JSON")
                return body
        except (aiohttp.ClientError, asyncio.TimeoutError) as err:
            raise BookClubConnectionError(f"Cannot reach BookClub at {self.url}: {err}") from err

    # ── read ──
    async def status(self) -> dict[str, Any]:
        data = await self._request("GET", "/api/status", auth=False)
        if not isinstance(data, dict) or "state" not in data:
            raise BookClubConnectionError("/api/status did not look like BookClub")
        return data

    async def requests(self, limit: int = 10) -> list[dict[str, Any]]:
        return await self._request("GET", "/api/requests", params={"limit": limit})

    async def get_request(self, request_id: int) -> dict[str, Any]:
        return await self._request("GET", f"/api/requests/{request_id}")

    async def wishlist(self) -> list[dict[str, Any]]:
        return await self._request("GET", "/api/wishlist")

    # ── requests ──
    async def request_book(self, query: str, requested_by: str = "", force: bool = False) -> dict[str, Any]:
        return await self._request(
            "POST", "/api/requests", json={"query": query, "requested_by": requested_by, "force": force}
        )

    async def choose(self, request_id: int, option: int | None = None, *, topic_id: str = "",
                     force: bool = False) -> dict[str, Any]:
        body: dict[str, Any] = {}
        if option is not None:
            body["option"] = option
        if topic_id:
            body["topic_id"] = topic_id
        if force:
            body["force"] = True
        return await self._request("POST", f"/api/requests/{request_id}/choose", json=body)

    async def cancel(self, request_id: int) -> dict[str, Any]:
        return await self._request("POST", f"/api/requests/{request_id}/cancel")

    # ── wishlist ──
    async def add_to_wishlist(self, query: str, kind: str = "book", author: str = "",
                              auto_download: bool = True, requested_by: str = "") -> dict[str, Any]:
        return await self._request(
            "POST", "/api/wishlist",
            json={"query": query, "kind": kind, "author": author,
                  "auto_download": auto_download, "requested_by": requested_by},
        )

    async def remove_from_wishlist(self, item_id: int) -> dict[str, Any]:
        return await self._request("DELETE", f"/api/wishlist/{item_id}")

    # ── integration webhook ──
    async def set_webhook(self, url: str) -> dict[str, Any]:
        return await self._request("PUT", "/api/integration/webhook", json={"url": url})

    async def clear_webhook(self) -> dict[str, Any]:
        return await self._request("DELETE", "/api/integration/webhook")
