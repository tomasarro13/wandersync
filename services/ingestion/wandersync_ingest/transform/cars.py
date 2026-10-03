"""Normalización de disponibilidad de autos."""

from __future__ import annotations

from collections import Counter
from datetime import datetime, timezone
from typing import Any

from ..tracing import step, traced


class CarNormalizer:
    def __init__(self, ingestion_run_id: str | None = None):
        self.ingestion_run_id = ingestion_run_id
        self.discarded: Counter[str] = Counter()

    @traced
    def normalize_batch(self, results: list[dict]) -> dict[str, Any]:
        now = datetime.now(timezone.utc).isoformat()
        rows: dict[str, dict] = {}
        for result in results:
            for v in result["vehicles"]:
                try:
                    if int(v["available"]) <= 0:
                        self.discarded["sin_unidades"] += 1
                        continue
                    rows[v["vehicleId"]] = {
                        "id": f"mock-rentals:{v['vehicleId']}",
                        "source": "mock-rentals",
                        "city": result["city"],
                        "destination_code": result["location"],
                        "pickup_date": result["pickup_date"],
                        "company": v["supplier"],
                        "model": v["model"],
                        "category": v["category"],
                        "transmission": v["transmission"],
                        "seats": int(v["seats"]),
                        "bags": int(v["bags"]),
                        "air_conditioning": bool(v.get("ac", True)),
                        "price_per_day": float(v["dailyRate"]["amount"]),
                        "currency": v["dailyRate"].get("currency", "COP"),
                        "units_available": int(v["available"]),
                        "scraped_at": now,
                        "ingestion_run_id": self.ingestion_run_id,
                    }
                except (KeyError, TypeError, ValueError):
                    self.discarded["estructura_invalida"] += 1
        step(f"{len(rows)} vehículos válidos · descartados={dict(self.discarded)}")
        return {"rows": list(rows.values()), "discarded": dict(self.discarded),
                "summary": f"{len(rows)} vehículos válidos"}
