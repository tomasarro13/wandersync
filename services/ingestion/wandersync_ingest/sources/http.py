"""Cliente HTTP base con clasificación de errores y trazado."""

from __future__ import annotations

import json
from typing import Any

import httpx

from ..config import settings
from ..tracing import step, traced
from .errors import MalformedPayloadError, PermanentSourceError, RateLimitedError, TransientSourceError


class HttpSourceClient:
    source_name = "http"

    def __init__(self, base_url: str, headers: dict[str, str] | None = None, timeout: float | None = None):
        self.base_url = base_url.rstrip("/")
        self._client = httpx.Client(
            base_url=self.base_url,
            headers=headers or {},
            timeout=timeout or settings.http_timeout,
            follow_redirects=True,
        )

    def close(self) -> None:
        self._client.close()

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        self.close()

    @traced(name="HttpSourceClient.request")
    def request(self, method: str, path: str, *, params: dict | None = None, json_body: Any = None) -> Any:
        step(f"{method} {self.base_url}{path} params={params or {}}")
        try:
            response = self._client.request(method, path, params=params, json=json_body)
        except httpx.TimeoutException as exc:
            raise TransientSourceError(f"timeout tras {self._client.timeout.read}s en {path}") from exc
        except httpx.TransportError as exc:
            raise TransientSourceError(f"error de red: {exc}") from exc

        step(f"HTTP {response.status_code} · {len(response.content) / 1024:.1f}KB · {response.elapsed.total_seconds():.2f}s")
        self._raise_for_status(response)
        return self.parse_json(response.text)

    def _raise_for_status(self, response: httpx.Response) -> None:
        code = response.status_code
        if code < 400:
            return
        body = response.text[:200]
        if code == 429:
            raise RateLimitedError(f"429 rate limit (Retry-After={response.headers.get('Retry-After')})")
        if code >= 500 or code in (408, 425):
            raise TransientSourceError(f"HTTP {code}: {body}")
        raise PermanentSourceError(f"HTTP {code}: {body}")

    @traced(name="HttpSourceClient.parse_json")
    def parse_json(self, text: str) -> Any:
        try:
            return json.loads(text)
        except json.JSONDecodeError as exc:
            raise MalformedPayloadError(f"JSON inválido en la posición {exc.pos}: {text[:80]!r}") from exc
