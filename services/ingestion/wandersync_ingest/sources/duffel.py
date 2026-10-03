"""Ofertas de vuelo reales del sandbox de Duffel (modo test, aerolínea "Duffel Airways" ZZ)."""

from __future__ import annotations

from typing import Any

from ..config import settings
from ..tracing import step, traced
from .errors import MalformedPayloadError, PermanentSourceError
from .http import HttpSourceClient


class DuffelFlightClient(HttpSourceClient):
    source_name = "duffel"

    def __init__(self) -> None:
        if not settings.duffel_access_token:
            raise PermanentSourceError("DUFFEL_ACCESS_TOKEN no está configurado")
        if not settings.duffel_access_token.startswith("duffel_test_"):
            raise PermanentSourceError("Por seguridad solo se aceptan tokens de prueba (duffel_test_...)")
        super().__init__(
            settings.duffel_api_url,
            headers={
                "Authorization": f"Bearer {settings.duffel_access_token}",
                "Duffel-Version": "v2",
                "Accept": "application/json",
                "Content-Type": "application/json",
            },
            timeout=max(settings.http_timeout, 30),
        )

    @traced
    def search_offers(self, origin: str, destination: str, date: str, max_offers: int = 10) -> dict[str, Any]:
        body = {
            "data": {
                "slices": [{"origin": origin, "destination": destination, "departure_date": date}],
                "passengers": [{"type": "adult"}],
                "cabin_class": "economy",
                "max_connections": 1,
            }
        }
        payload = self.request("POST", "/air/offer_requests", params={"return_offers": "true", "supplier_timeout": 20000}, json_body=body)
        offers = self.extract_offers(payload)
        offers.sort(key=lambda o: float(o.get("total_amount") or 0))
        kept = offers[:max_offers]
        step(f"{origin}→{destination} {date}: {len(offers)} ofertas recibidas, se conservan las {len(kept)} más baratas")
        return {"origin": origin, "destination": destination, "date": date, "offers": kept,
                "summary": f"{origin}→{destination} {date}: {len(kept)} ofertas"}

    @traced
    def extract_offers(self, payload: Any) -> list[dict[str, Any]]:
        try:
            offers = payload["data"]["offers"]
        except (KeyError, TypeError) as exc:
            raise MalformedPayloadError("respuesta de Duffel sin data.offers") from exc
        if not isinstance(offers, list):
            raise MalformedPayloadError("data.offers no es una lista")
        return offers
