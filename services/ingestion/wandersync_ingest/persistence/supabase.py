"""Persistencia en Supabase vía PostgREST (HTTPS).

Se usa la API REST en lugar de una conexión Postgres directa porque:
  * funciona desde Docker sin depender de IPv6 (la conexión directa de Supabase es solo IPv6),
  * el upsert masivo es una sola petición por lote (`Prefer: resolution=merge-duplicates`),
  * la misma base expone automáticamente GraphQL (pg_graphql) para el resto del sistema.

Sin credenciales configuradas el repositorio entra en modo *dry-run*: registra lo que
habría escrito y continúa, para poder probar el pipeline completo sin Supabase.
"""

from __future__ import annotations

from typing import Any

import httpx

from ..config import settings
from ..sources.errors import PermanentSourceError, TransientSourceError
from ..tracing import step, traced


class SupabaseRepository:
    def __init__(self) -> None:
        self.dry_run = not settings.supabase_enabled
        if self.dry_run:
            self._client = None
            return
        key = settings.supabase_secret_key or ""
        headers = {"apikey": key, "Content-Type": "application/json"}
        # Las claves legacy (service_role) son JWT y van también en Authorization;
        # las nuevas (sb_secret_...) solo en `apikey`.
        if key.startswith("eyJ"):
            headers["Authorization"] = f"Bearer {key}"
        self._client = httpx.Client(base_url=f"{settings.supabase_url.rstrip('/')}/rest/v1", headers=headers, timeout=30)

    def close(self) -> None:
        if self._client:
            self._client.close()

    @traced
    def upsert(self, table: str, rows: list[dict], on_conflict: str = "id") -> dict[str, Any]:
        if not rows:
            return {"table": table, "rows": 0, "summary": f"{table}: nada que escribir"}
        if self.dry_run:
            step(f"[dry-run] Supabase no configurado: se omiten {len(rows)} filas en {table}")
            return {"table": table, "rows": len(rows), "dry_run": True, "summary": f"{table}: {len(rows)} filas (dry-run)"}
        response = self._send(
            "POST", f"/{table}", params={"on_conflict": on_conflict},
            json=rows, headers={"Prefer": "resolution=merge-duplicates,return=minimal"},
        )
        step(f"PostgREST {response.status_code} · {table} ← {len(rows)} filas")
        return {"table": table, "rows": len(rows), "summary": f"{table}: {len(rows)} filas upsert"}

    @traced
    def insert_returning(self, table: str, row: dict) -> dict | None:
        if self.dry_run:
            return None
        response = self._send("POST", f"/{table}", json=row, headers={"Prefer": "return=representation"})
        data = response.json()
        return data[0] if data else None

    @traced
    def update(self, table: str, match: dict[str, str], values: dict) -> None:
        if self.dry_run:
            return
        params = {k: f"eq.{v}" for k, v in match.items()}
        self._send("PATCH", f"/{table}", params=params, json=values, headers={"Prefer": "return=minimal"})

    @staticmethod
    def _postgres_code(response: httpx.Response) -> str:
        try:
            return str(response.json().get("code") or "")
        except Exception:
            return ""

    def _send(self, method: str, path: str, **kwargs) -> httpx.Response:
        try:
            response = self._client.request(method, path, **kwargs)
        except httpx.TransportError as exc:
            raise TransientSourceError(f"Supabase inaccesible: {exc}") from exc
        # Errores de datos de Postgres (cardinalidad, integridad, columna inexistente…) no se arreglan reintentando.
        if self._postgres_code(response)[:2] in ("21", "22", "23", "42"):
            raise PermanentSourceError(f"Supabase HTTP {response.status_code}: {response.text[:300]}")
        if response.status_code == 429 or response.status_code >= 500:
            raise TransientSourceError(f"Supabase HTTP {response.status_code}: {response.text[:200]}")
        if response.status_code >= 400:
            raise PermanentSourceError(f"Supabase HTTP {response.status_code}: {response.text[:300]}")
        return response
