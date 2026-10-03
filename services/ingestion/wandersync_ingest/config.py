"""Configuración centralizada (variables de entorno) y catálogo de destinos/rutas."""

from __future__ import annotations

import os
from dataclasses import dataclass, field


def _env(name: str, default: str | None = None) -> str | None:
    value = os.getenv(name)
    return value if value not in (None, "") else default


@dataclass(frozen=True)
class Destination:
    code: str      # IATA, enlaza hoteles ↔ vuelos ↔ autos
    city: str
    tb_slug: str   # slug de TiquetesBaratos
    tb_id: str     # id de destino en TiquetesBaratos (alojamientos-en-<slug>_d<id>)


DESTINATIONS: dict[str, Destination] = {
    d.code: d
    for d in [
        Destination("CTG", "Cartagena", "cartagena", "167953"),
        Destination("ADZ", "San Andrés", "san-andres", "80744"),
        Destination("SMR", "Santa Marta", "santa-marta", "221814"),
        Destination("MDE", "Medellín", "medellin", "167960"),
        Destination("BOG", "Bogotá", "bogota", "54198"),
    ]
}

CITY_BY_CODE = {
    "BOG": "Bogotá", "MDE": "Medellín", "CTG": "Cartagena", "ADZ": "San Andrés",
    "SMR": "Santa Marta", "CLO": "Cali", "BAQ": "Barranquilla", "PEI": "Pereira", "LET": "Leticia",
}

# BOG-LET y LET están "envenenados" en el mock: siempre fallan → demuestran reintentos agotados.
DEFAULT_ROUTES = ["BOG-CTG", "BOG-ADZ", "BOG-SMR", "BOG-MDE", "MDE-CTG", "MDE-ADZ", "CLO-CTG", "BOG-LET"]
DEFAULT_DUFFEL_ROUTES = ["BOG-CTG", "BOG-ADZ", "MDE-CTG"]
DEFAULT_CAR_LOCATIONS = ["CTG", "ADZ", "SMR", "MDE", "BOG", "LET"]


@dataclass(frozen=True)
class Settings:
    dask_scheduler_address: str = field(default_factory=lambda: _env("DASK_SCHEDULER_ADDRESS", "tcp://dask-scheduler:8786"))

    supabase_url: str | None = field(default_factory=lambda: _env("SUPABASE_URL"))
    supabase_secret_key: str | None = field(default_factory=lambda: _env("SUPABASE_SECRET_KEY"))

    duffel_access_token: str | None = field(default_factory=lambda: _env("DUFFEL_ACCESS_TOKEN"))
    duffel_api_url: str = field(default_factory=lambda: _env("DUFFEL_API_URL", "https://api.duffel.com"))

    mock_sources_url: str = field(default_factory=lambda: _env("MOCK_SOURCES_URL", "http://mock-sources:4010"))
    tiquetesbaratos_url: str = field(default_factory=lambda: _env("TIQUETESBARATOS_URL", "https://www.tiquetesbaratos.com"))
    scraper_user_agent: str = field(
        default_factory=lambda: _env(
            "SCRAPER_USER_AGENT",
            "WanderSyncAcademicBot/1.0 (+proyecto academico Universidad de La Sabana; bajo volumen)",
        )
    )
    # Pausa de cortesía entre peticiones a TiquetesBaratos (segundos)
    scraper_polite_delay: float = field(default_factory=lambda: float(_env("SCRAPER_POLITE_DELAY", "0.6")))
    http_timeout: float = field(default_factory=lambda: float(_env("HTTP_TIMEOUT_SECONDS", "8")))

    # Conversión a COP (tasas fijas de referencia, configurables)
    fx_to_cop: dict[str, float] = field(
        default_factory=lambda: {
            "COP": 1.0,
            "USD": float(_env("FX_USD_COP", "4000")),
            "EUR": float(_env("FX_EUR_COP", "4500")),
            "GBP": float(_env("FX_GBP_COP", "5200")),
        }
    )

    # Emitir un evento de Prefect por cada función trazada (pestaña "Events" de la UI)
    trace_events: bool = field(default_factory=lambda: _env("TRACE_EVENTS", "true").lower() == "true")

    @property
    def supabase_enabled(self) -> bool:
        return bool(self.supabase_url and self.supabase_secret_key)

    @property
    def duffel_enabled(self) -> bool:
        return bool(self.duffel_access_token)


settings = Settings()
