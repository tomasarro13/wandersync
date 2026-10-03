"""Extracción real de hoteles desde los endpoints públicos /ai/ de TiquetesBaratos.

TiquetesBaratos publica en https://www.tiquetesbaratos.com/llms.txt unos endpoints `/ai/...`
pensados para consumo automatizado, sin autenticación y con rate limiting estándar.
Para ser buenos ciudadanos: User-Agent identificable, pausa entre peticiones,
límite de concurrencia (tag "tiquetesbaratos" en Prefect) y pocas páginas por destino.
"""

from __future__ import annotations

import time
from typing import Any

from ..config import Destination, settings
from ..tracing import step, traced
from .errors import MalformedPayloadError
from .http import HttpSourceClient


class TiquetesBaratosHotelScraper(HttpSourceClient):
    source_name = "tiquetesbaratos"

    def __init__(self) -> None:
        super().__init__(
            settings.tiquetesbaratos_url,
            headers={"User-Agent": settings.scraper_user_agent, "Accept": "application/json"},
            timeout=max(settings.http_timeout, 15),
        )

    def _polite_pause(self) -> None:
        if settings.scraper_polite_delay > 0:
            time.sleep(settings.scraper_polite_delay)

    @traced
    def build_listing_path(self, destination: Destination, page: int) -> str:
        base = f"/ai/es/alojamientos-en-{destination.tb_slug}_d{destination.tb_id}"
        return base if page <= 1 else f"{base}/page_{page}"

    @traced
    def fetch_listing_page(self, destination: Destination, page: int, checkin: str, checkout: str) -> dict[str, Any]:
        self._polite_pause()
        path = self.build_listing_path(destination, page)
        payload = self.request(
            "GET",
            path,
            params={"checkin": checkin, "checkout": checkout, "group_adults": 2, "no_rooms": 1, "format": "json", "currency": "COP"},
        )
        return self.validate_listing(payload, destination, page)

    @traced
    def validate_listing(self, payload: Any, destination: Destination, page: int) -> dict[str, Any]:
        if not isinstance(payload, dict) or not isinstance(payload.get("hotels"), list):
            raise MalformedPayloadError("la respuesta no contiene la lista 'hotels'")
        hotels = payload["hotels"]
        step(
            f"{payload.get('destination_name')}: página {page}/{payload.get('total_pages')} · "
            f"{len(hotels)} hoteles en esta página · {payload.get('total_hotels')} en total"
        )
        return {
            "destination_code": destination.code,
            "page": page,
            "total_pages": payload.get("total_pages"),
            "total_hotels": payload.get("total_hotels"),
            "hotels": hotels,
            "summary": f"{destination.city} p{page}: {len(hotels)} hoteles",
        }

    @traced
    def fetch_hotel_detail(self, booking_url: str, checkin: str, checkout: str) -> dict[str, Any]:
        self._polite_pause()
        path = self.detail_path_from_booking_url(booking_url)
        payload = self.request("GET", path, params={"checkin": checkin, "checkout": checkout, "format": "json", "currency": "COP"})
        hotel = payload.get("hotel") if isinstance(payload, dict) else None
        if not isinstance(hotel, dict):
            raise MalformedPayloadError("la respuesta de detalle no contiene 'hotel'")
        rooms = [r.get("name") for r in hotel.get("rooms") or [] if isinstance(r, dict) and r.get("name")]
        step(f"{hotel.get('name')}: {len(rooms)} tipos de habitación · todo incluido={hotel.get('is_all_inclusive')}")
        return {
            "booking_url": booking_url,
            "room_types": rooms,
            "is_all_inclusive": bool(hotel.get("is_all_inclusive")),
            "summary": f"{hotel.get('name')}: {len(rooms)} habitaciones",
        }

    @traced
    def detail_path_from_booking_url(self, booking_url: str) -> str:
        # https://www.tiquetesbaratos.com/es/hotel/<slug>_a<id>  →  /ai/es/hotel/<slug>_a<id>
        marker = "/es/hotel/"
        if marker not in booking_url:
            raise MalformedPayloadError(f"booking_url inesperada: {booking_url}")
        return "/ai" + booking_url[booking_url.index(marker):]
