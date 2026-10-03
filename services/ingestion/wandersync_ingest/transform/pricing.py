"""Motor de tarifas hoteleras distribuido con Dask Bag.

TiquetesBaratos entrega el catálogo real pero sin precios, así que las tarifas por noche
se calculan aquí: hotel × tipo de habitación × fecha.  El trabajo se reparte en particiones
de un Dask Bag que se ejecutan en paralelo en los workers del clúster; cada partición
reporta qué worker la procesó, para poder mostrarlo en Prefect y en el dashboard de Dask.
"""

from __future__ import annotations

import hashlib
import time
from datetime import date, datetime, timedelta, timezone
from typing import Any

import dask.bag as db

from ..tracing import current_logger, step, traced, worker_name

BASE_BY_STARS = {1: 85000, 2: 130000, 3: 210000, 4: 360000, 5: 620000}
DESTINATION_FACTOR = {"ADZ": 1.25, "CTG": 1.2, "SMR": 1.1, "MDE": 1.0, "BOG": 0.95}
ROOM_KEYWORDS = [("suite", 1.6), ("famil", 1.35), ("junior", 1.3), ("deluxe", 1.25), ("superior", 1.15), ("vista", 1.1)]


def _noise(*parts: Any) -> float:
    """Ruido determinista en [0, 1) — la misma combinación siempre da el mismo precio."""
    digest = hashlib.sha256("|".join(map(str, parts)).encode()).digest()
    return int.from_bytes(digest[:4], "big") / 2**32


def _season_factor(day: date) -> float:
    md = (day.month, day.day)
    if md >= (12, 15) or md <= (1, 15) or ((6, 15) <= md <= (7, 31)):
        return 1.3
    return 1.0


def _room_factor(room_type: str, index: int) -> float:
    lower = room_type.lower()
    for keyword, factor in ROOM_KEYWORDS:
        if keyword in lower:
            return factor
    return 1.0 + 0.08 * index


def price_partition(items: list[dict], start: str, days: int) -> list[dict]:
    """Se ejecuta DENTRO de un worker de Dask (fuera del contexto de Prefect).
    Sus logs van al worker: `docker compose logs dask-worker-N` / dashboard → Info → Logs."""
    log = current_logger()
    worker = worker_name()
    first = items[0]["hotel_id"] if items else "-"
    log.info(f"▶ ENTRA  price_partition(items={len(items)}, desde={first}) · worker={worker}")
    started = time.perf_counter()
    start_day = date.fromisoformat(start)
    computed_at = datetime.now(timezone.utc).isoformat()
    rates: list[dict] = []
    for item in items:
        stars = int(round(item["stars"] or 3))
        base = BASE_BY_STARS.get(min(max(stars, 1), 5), 210000)
        base *= DESTINATION_FACTOR.get(item["destination_code"], 1.0)
        base *= 1 + ((item["rating"] or 7.0) - 7.0) * 0.04
        base *= _room_factor(item["room_type"], item["room_index"])
        if item["is_all_inclusive"]:
            base *= 1.8
        for offset in range(days):
            day = start_day + timedelta(days=offset)
            weekend = 1.15 if day.weekday() in (4, 5) else 1.0
            noise = 0.9 + 0.2 * _noise(item["hotel_id"], item["room_type"], day)
            price = round(base * weekend * _season_factor(day) * noise / 100) * 100
            rates.append({
                "id": f"{item['hotel_id']}:{item['room_type']}:{day.isoformat()}",
                "hotel_id": item["hotel_id"],
                "stay_date": day.isoformat(),
                "room_type": item["room_type"],
                "price_per_night": price,
                "currency": "COP",
                "rooms_available": int(_noise("stock", item["hotel_id"], item["room_type"], day) * 9),
                "computed_at": computed_at,
            })
    elapsed = time.perf_counter() - started
    log.info(f"◀ SALE   price_partition → {len(rates)} tarifas · {elapsed:.2f}s · worker={worker}")
    return [{"worker": worker, "items": len(items), "rates": rates, "seconds": round(elapsed, 3)}]


class HotelRateEngine:
    @traced
    def build_work_items(self, hotels: list[dict]) -> list[dict]:
        items = [
            {
                "hotel_id": h["id"],
                "destination_code": h["destination_code"],
                "stars": h.get("stars"),
                "rating": h.get("rating"),
                "is_all_inclusive": h.get("is_all_inclusive", False),
                "room_type": room,
                "room_index": i,
            }
            for h in hotels
            for i, room in enumerate(h.get("room_types") or ["Estándar"])
        ]
        step(f"{len(hotels)} hoteles → {len(items)} combinaciones hotel×habitación")
        return items

    @traced
    def compute_distributed(self, client, hotels: list[dict], start: str, days: int) -> dict[str, Any]:
        items = self.build_work_items(hotels)
        if not items:
            return {"rates": [], "partitions": [], "summary": "sin hoteles"}
        npartitions = max(3, min(24, len(items) // 8))
        bag = db.from_sequence(items, npartitions=npartitions)
        step(f"Dask Bag creado: {len(items)} elementos en {npartitions} particiones → enviando al clúster")
        priced = bag.map_partitions(price_partition, start, days, token="HotelRateEngine.price_partition")
        partitions = client.compute(priced).result()
        rates = [r for p in partitions for r in p["rates"]]
        by_worker: dict[str, int] = {}
        for i, p in enumerate(partitions):
            by_worker[p["worker"]] = by_worker.get(p["worker"], 0) + 1
            step(f"partición {i:02d} → {p['worker']} · {p['items']} combinaciones · {len(p['rates'])} tarifas · {p['seconds']}s")
        step(f"reparto por worker: {by_worker}")
        return {
            "rates": rates,
            "partitions": [{k: v for k, v in p.items() if k != "rates"} for p in partitions],
            "by_worker": by_worker,
            "summary": f"{len(rates)} tarifas en {npartitions} particiones sobre {len(by_worker)} workers",
        }
