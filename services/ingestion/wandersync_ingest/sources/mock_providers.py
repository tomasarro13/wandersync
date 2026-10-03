"""Clientes de los proveedores simulados (mock-sources): aerolíneas colombianas y alquiler de autos."""

from __future__ import annotations

from typing import Any

from ..config import settings
from ..tracing import step, traced
from .errors import MalformedPayloadError
from .http import HttpSourceClient


class MockAirlineFeedClient(HttpSourceClient):
    source_name = "mock-airlines"

    def __init__(self) -> None:
        super().__init__(settings.mock_sources_url)

    @traced
    def search_itineraries(self, origin: str, destination: str, date: str) -> dict[str, Any]:
        payload = self.request("GET", "/airlines/v1/itineraries", params={"origin": origin, "destination": destination, "date": date})
        if not isinstance(payload, dict) or not isinstance(payload.get("itineraries"), list):
            raise MalformedPayloadError("respuesta sin 'itineraries'")
        items = payload["itineraries"]
        step(f"{origin}→{destination} {date}: {len(items)} itinerarios · {payload.get('distanceKm')} km")
        return {"origin": origin, "destination": destination, "date": date, "itineraries": items,
                "summary": f"{origin}→{destination} {date}: {len(items)} itinerarios"}


class MockCarRentalClient(HttpSourceClient):
    source_name = "mock-rentals"

    def __init__(self) -> None:
        super().__init__(settings.mock_sources_url)

    @traced
    def search_availability(self, location: str, pickup_date: str) -> dict[str, Any]:
        payload = self.request("GET", "/rentals/v1/availability", params={"location": location, "pickupDate": pickup_date})
        if not isinstance(payload, dict) or not isinstance(payload.get("vehicles"), list):
            raise MalformedPayloadError("respuesta sin 'vehicles'")
        vehicles = payload["vehicles"]
        step(f"{payload.get('city')} {pickup_date}: {len(vehicles)} vehículos de {len({v.get('supplier') for v in vehicles})} proveedores")
        return {"location": location, "city": payload.get("city"), "pickup_date": pickup_date, "vehicles": vehicles,
                "summary": f"{location} {pickup_date}: {len(vehicles)} vehículos"}
