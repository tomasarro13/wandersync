"""Limpieza y estructuración de hoteles crudos de TiquetesBaratos."""

from __future__ import annotations

import html
import re
from collections import Counter
from datetime import datetime, timezone
from typing import Any

from ..config import Destination
from ..tracing import step, traced

DEFAULT_ROOM_TYPES = ["Estándar", "Superior"]
_STARS = re.compile(r"(\d+(?:[.,]\d)?)")


class HotelCleaner:
    def __init__(self, ingestion_run_id: str | None = None):
        self.ingestion_run_id = ingestion_run_id
        self.discarded: Counter[str] = Counter()

    @traced
    def clean_destination(self, destination: Destination, pages: list[dict], details: list[dict]) -> dict[str, Any]:
        raw = [h for page in pages for h in page.get("hotels", [])]
        step(f"{destination.city}: {len(raw)} registros crudos de {len(pages)} páginas, {len(details)} detalles")
        unique = self.deduplicate(raw)
        details_by_url = {d["booking_url"]: d for d in details}
        rows = [row for h in unique if (row := self.to_row(h, destination, details_by_url.get(h.get("booking_url")))) is not None]
        step(f"{destination.city}: {len(rows)} hoteles válidos · descartados={dict(self.discarded)}")
        return {
            "destination_code": destination.code,
            "rows": rows,
            "raw": len(raw),
            "discarded": dict(self.discarded),
            "summary": f"{destination.city}: {len(rows)} válidos de {len(raw)} crudos",
        }

    @traced
    def deduplicate(self, hotels: list[dict]) -> list[dict]:
        seen: dict[str, dict] = {}
        for h in hotels:
            hid = str(h.get("id") or "")
            if not hid:
                self.discarded["sin_id"] += 1
                continue
            if hid in seen:
                self.discarded["duplicado"] += 1
                continue
            seen[hid] = h
        return list(seen.values())

    def to_row(self, h: dict, destination: Destination, detail: dict | None) -> dict | None:
        name = html.unescape((h.get("name") or "").strip())
        if not name:
            self.discarded["sin_nombre"] += 1
            return None
        location = h.get("location") or {}
        lat, lng = self.parse_float(location.get("lat")), self.parse_float(location.get("lng"))
        if lat is None or lng is None:
            self.discarded["sin_coordenadas"] += 1
            return None
        rating = h.get("rating") or {}
        # Algunos hoteles repiten el nombre de una habitación: se deduplican conservando el orden.
        room_types = list(dict.fromkeys(r.strip() for r in (detail or {}).get("room_types") or DEFAULT_ROOM_TYPES if r.strip()))
        return {
            "id": f"tb:{h['id']}",
            "source": "tiquetesbaratos",
            "external_id": str(h["id"]),
            "name": name,
            "city": location.get("city") or destination.city,
            "destination_code": destination.code,
            "stars": self.parse_stars(h.get("category")),
            "rating": self.parse_float(rating.get("score")),
            "reviews": int(rating.get("reviews") or 0),
            "lat": lat,
            "lng": lng,
            "thumbnail": h.get("thumbnail"),
            "amenities": [html.unescape(a) for a in h.get("amenities") or [] if isinstance(a, str)],
            "highlights": [html.unescape(a) for a in h.get("highlights") or [] if isinstance(a, str)],
            "room_types": room_types[:6],
            "is_all_inclusive": bool((detail or {}).get("is_all_inclusive")) or "todo incluido" in name.lower() or "all inclusive" in name.lower(),
            "booking_url": h.get("booking_url"),
            "scraped_at": datetime.now(timezone.utc).isoformat(),
            "ingestion_run_id": self.ingestion_run_id,
        }

    @staticmethod
    def parse_float(value: Any) -> float | None:
        try:
            return float(str(value).replace(",", "."))
        except (TypeError, ValueError):
            return None

    @staticmethod
    def parse_stars(category: Any) -> float | None:
        match = _STARS.search(str(category or ""))
        return float(match.group(1).replace(",", ".")) if match else None
