"""Normalización de ofertas de vuelo (Duffel + feed mock) y estadísticas por ruta con Dask DataFrame."""

from __future__ import annotations

import re
from collections import Counter
from datetime import datetime, timezone
from typing import Any

import dask.dataframe as dd
import pandas as pd

from ..config import settings
from ..tracing import step, traced

_ISO_DURATION = re.compile(r"P(?:(\d+)D)?T?(?:(\d+)H)?(?:(\d+)M)?")


def iso_duration_minutes(value: str | None) -> int | None:
    match = _ISO_DURATION.fullmatch(value or "")
    if not match or not any(match.groups()):
        return None
    d, h, m = (int(x or 0) for x in match.groups())
    return d * 1440 + h * 60 + m


class FlightNormalizer:
    def __init__(self, ingestion_run_id: str | None = None):
        self.ingestion_run_id = ingestion_run_id
        self.discarded: Counter[str] = Counter()
        self.now = datetime.now(timezone.utc).isoformat()

    @traced
    def normalize_batch(self, mock_results: list[dict], duffel_results: list[dict]) -> dict[str, Any]:
        rows: dict[str, dict] = {}
        for result in mock_results:
            for it in result["itineraries"]:
                if (row := self.from_mock(it)) is not None:
                    rows[row["id"]] = row
        step(f"feed mock: {sum(len(r['itineraries']) for r in mock_results)} itinerarios procesados")
        for result in duffel_results:
            for offer in result["offers"]:
                if (row := self.from_duffel(offer, result["origin"], result["destination"])) is not None:
                    rows[row["id"]] = row
        step(f"duffel: {sum(len(r['offers']) for r in duffel_results)} ofertas procesadas")
        valid = [r for r in rows.values() if self.validate(r)]
        return {"rows": valid, "discarded": dict(self.discarded),
                "summary": f"{len(valid)} vuelos válidos · descartados={dict(self.discarded)}"}

    def from_mock(self, it: dict) -> dict | None:
        try:
            return {
                "id": f"mock-airlines:{it['itineraryId']}",
                "source": "mock-airlines",
                "origin": it["departure"]["airport"],
                "destination": it["arrival"]["airport"],
                "departure_at": it["departure"]["time"],
                "arrival_at": it["arrival"]["time"],
                "duration_minutes": it["durationMinutes"],
                "airline_code": it["carrier"]["code"],
                "airline_name": it["carrier"]["name"],
                "flight_number": it["flightNumber"],
                "stops": it.get("stops", 0),
                "cabin_class": (it["fare"].get("cabin") or "economy").lower(),
                "price": float(it["fare"]["amount"]),
                "currency": "COP",
                "original_price": float(it["fare"]["amount"]),
                "original_currency": it["fare"]["currency"],
                "seats_available": it.get("seatsLeft"),
                "external_offer_id": it["itineraryId"],
                "scraped_at": self.now,
                "ingestion_run_id": self.ingestion_run_id,
            }
        except (KeyError, TypeError, ValueError):
            self.discarded["estructura_invalida"] += 1
            return None

    def from_duffel(self, offer: dict, origin: str, destination: str) -> dict | None:
        try:
            sl = offer["slices"][0]
            segments = sl["segments"]
            first, last = segments[0], segments[-1]
            carrier = first.get("marketing_carrier") or offer.get("owner") or {}
            number = f"{carrier.get('iata_code', 'ZZ')}{first.get('marketing_carrier_flight_number', '')}"
            amount = float(offer["total_amount"])
            currency = offer["total_currency"]
            rate = settings.fx_to_cop.get(currency)
            if rate is None:
                self.discarded["moneda_desconocida"] += 1
                return None
            # Los timestamps de Duffel vienen en hora local del aeropuerto sin offset → se asume Colombia (UTC-5).
            dep = first["departing_at"] + ("" if "+" in first["departing_at"] else "-05:00")
            arr = last["arriving_at"] + ("" if "+" in last["arriving_at"] else "-05:00")
            return {
                "id": f"duffel:{number}:{origin}{destination}:{first['departing_at']}",
                "source": "duffel",
                "origin": origin,
                "destination": destination,
                "departure_at": dep,
                "arrival_at": arr,
                "duration_minutes": iso_duration_minutes(sl.get("duration")),
                "airline_code": carrier.get("iata_code"),
                "airline_name": carrier.get("name"),
                "flight_number": number,
                "stops": len(segments) - 1,
                "cabin_class": "economy",
                "price": round(amount * rate / 100) * 100,
                "currency": "COP",
                "original_price": amount,
                "original_currency": currency,
                "seats_available": None,
                "external_offer_id": offer.get("id"),
                "scraped_at": self.now,
                "ingestion_run_id": self.ingestion_run_id,
            }
        except (KeyError, IndexError, TypeError, ValueError):
            self.discarded["estructura_invalida"] += 1
            return None

    def validate(self, row: dict) -> bool:
        if row["price"] <= 0:
            self.discarded["precio_invalido"] += 1
            return False
        if row["departure_at"] >= row["arrival_at"] and row["source"] != "duffel":
            self.discarded["horario_invalido"] += 1
            return False
        if row["seats_available"] == 0:
            self.discarded["sin_cupo"] += 1
            return False
        return True


class RouteStatsCalculator:
    @traced
    def compute(self, client, rows: list[dict]) -> dict[str, Any]:
        if not rows:
            return {"rows": [], "summary": "sin vuelos"}
        pdf = pd.DataFrame(rows)[["id", "origin", "destination", "price"]]
        npartitions = max(2, min(8, len(pdf) // 25))
        ddf = dd.from_pandas(pdf, npartitions=npartitions)
        step(f"Dask DataFrame: {len(pdf)} filas en {npartitions} particiones → groupby(origin, destination)")
        agg = ddf.groupby(["origin", "destination"]).price.agg(["min", "mean", "max", "count"])
        stats = client.compute(agg).result().reset_index()
        cheapest_idx = client.compute(ddf.groupby(["origin", "destination"]).price.idxmin()).result()
        cheapest_by_route = {f"{o}-{d}": pdf.at[idx, "id"] for (o, d), idx in cheapest_idx.items()}
        now = datetime.now(timezone.utc).isoformat()
        out = []
        for r in stats.itertuples():
            route = f"{r.origin}-{r.destination}"
            out.append({
                "route": route, "origin": r.origin, "destination": r.destination,
                "min_price": float(r.min), "avg_price": round(float(r.mean), 2), "max_price": float(r.max),
                "offers": int(r.count), "cheapest_flight_id": cheapest_by_route.get(route), "computed_at": now,
            })
            step(f"{route}: desde ${r.min:,.0f} · promedio ${r.mean:,.0f} · {r.count} ofertas")
        return {"rows": out, "summary": f"{len(out)} rutas agregadas"}
