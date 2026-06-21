import time
from typing import Any, Self

import httpx

_RETRYABLE_STATUSES = frozenset({429, 500, 502, 503, 504})
_MAX_RETRIES = 3
_BACKOFF_BASE = 2.0


class HttpError(Exception):
    def __init__(self, status_code: int, url: str) -> None:
        super().__init__(f"HTTP {status_code}: {url}")
        self.status_code = status_code


class HttpClient:
    def __init__(
        self,
        base_url: str = "",
        *,
        timeout: float = 30.0,
        headers: dict[str, str] | None = None,
        max_retries: int = _MAX_RETRIES,
    ) -> None:
        self._base_url = base_url.rstrip("/")
        self._max_retries = max_retries
        self._session = httpx.Client(
            timeout=timeout,
            headers=headers or {},
            follow_redirects=True,
        )

    def _build_url(self, path: str) -> str:
        if path.startswith(("http://", "https://")) or not self._base_url:
            return path
        return f"{self._base_url}/{path.lstrip('/')}" if path else self._base_url

    def _request(self, path: str, *, params: dict[str, Any] | None = None) -> httpx.Response:
        url = self._build_url(path)
        last_exc: Exception | None = None

        for attempt in range(self._max_retries):
            is_last = attempt == self._max_retries - 1
            try:
                response = self._session.get(url, params=params)
            except httpx.TransportError as exc:
                last_exc = exc
                if not is_last:
                    time.sleep(_BACKOFF_BASE**attempt)
                continue

            if response.status_code in _RETRYABLE_STATUSES and not is_last:
                wait = float(response.headers.get("Retry-After", _BACKOFF_BASE**attempt))
                time.sleep(wait)
                continue

            if not response.is_success:
                raise HttpError(response.status_code, url)

            return response

        raise HttpError(0, url) from last_exc

    def get_json(self, path: str = "", *, params: dict[str, Any] | None = None) -> Any:
        return self._request(path, params=params).json()

    def get_text(self, path: str = "", *, params: dict[str, Any] | None = None) -> str:
        return self._request(path, params=params).text

    def close(self) -> None:
        self._session.close()

    def __enter__(self) -> Self:
        return self

    def __exit__(self, *_: object) -> None:
        self.close()
