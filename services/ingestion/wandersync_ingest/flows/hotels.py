"""Flow de hoteles: TiquetesBaratos (real) → limpieza → motor de tarifas (Dask Bag) → Supabase.

Jerarquía visible en la UI de Prefect:
  ingesta-hoteles
   ├─ hoteles-1-extraccion      (páginas de listado + detalle, en paralelo en Dask)
   ├─ hoteles-2-transformacion  (limpieza por destino + tarifas distribuidas)
   └─ hoteles-3-carga           (upsert por lotes en paralelo)
"""

from __future__ import annotations

from datetime import date, timedelta
from typing import Any

from prefect import flow, get_run_logger, task
from prefect.artifacts import create_markdown_artifact, create_table_artifact
from prefect.cache_policies import NO_CACHE
from prefect.runtime import task_run
from prefect_dask import get_dask_client

from ..config import DESTINATIONS
from ..runner import TRANSIENT_RETRY, dask_runner
from ..sources.tiquetesbaratos import TiquetesBaratosHotelScraper
from ..transform.hotels import HotelCleaner
from ..transform.pricing import HotelRateEngine
from . import handoff
from .common import collect, extraction_table, load_table, register_run_finish, register_run_start, with_run_meta


# ----------------------------------------------------------------------------- tasks
@task(
    name="TiquetesBaratosHotelScraper.fetch_listing_page",
    task_run_name="TiquetesBaratosHotelScraper.fetch_listing_page[{destination_code}·p{page}]",
    tags=["tiquetesbaratos", "extraccion"],
    timeout_seconds=90,
    cache_policy=NO_CACHE,
    **TRANSIENT_RETRY,
)
def fetch_listing_page(destination_code: str, page: int, checkin: str, checkout: str) -> dict[str, Any]:
    with TiquetesBaratosHotelScraper() as scraper:
        result = scraper.fetch_listing_page(DESTINATIONS[destination_code], page, checkin, checkout)
    result["recurso"] = f"{destination_code}·p{page}"
    return with_run_meta(result)


def _detail_run_name() -> str:
    params = task_run.parameters
    return f"TiquetesBaratosHotelScraper.fetch_hotel_detail[{params['destination_code']}·{params['hotel_name'][:32]}]"


@task(
    name="TiquetesBaratosHotelScraper.fetch_hotel_detail",
    task_run_name=_detail_run_name,
    tags=["tiquetesbaratos", "extraccion"],
    timeout_seconds=90,
    cache_policy=NO_CACHE,
    **TRANSIENT_RETRY,
)
def fetch_hotel_detail(destination_code: str, hotel_name: str, booking_url: str, checkin: str, checkout: str) -> dict[str, Any]:
    with TiquetesBaratosHotelScraper() as scraper:
        result = scraper.fetch_hotel_detail(booking_url, checkin, checkout)
    result["recurso"] = f"{destination_code}·{hotel_name[:40]}"
    return with_run_meta(result)


@task(
    name="HotelCleaner.clean_destination",
    task_run_name="HotelCleaner.clean_destination[{destination_code}]",
    tags=["transformacion"],
    cache_policy=NO_CACHE,
)
def clean_destination(destination_code: str, pages: list[dict], details: list[dict], run_id: str | None) -> dict[str, Any]:
    result = HotelCleaner(run_id).clean_destination(DESTINATIONS[destination_code], pages, details)
    return with_run_meta(result)


@task(
    name="HotelRateEngine.compute_distributed",
    task_run_name="HotelRateEngine.compute_distributed[{days}dias·desde{start}]",
    tags=["transformacion", "dask-bag"],
    cache_policy=NO_CACHE,
)
def compute_hotel_rates(hotels: list[dict], start: str, days: int) -> dict[str, Any]:
    # Cliente temporal hacia el clúster completo: las particiones del Bag se reparten entre todos los workers.
    with get_dask_client() as client:
        result = HotelRateEngine().compute_distributed(client, hotels, start, days)
    return with_run_meta(result)


# ----------------------------------------------------------------------------- subflows
@flow(name="hoteles-1-extraccion", flow_run_name="hoteles·extraccion·{checkin}", task_runner=dask_runner(), log_prints=True)
def extract_hotels(destination_codes: list[str], pages_per_destination: int, details_per_destination: int, checkin: str, checkout: str) -> str:
    logger = get_run_logger()
    logger.info(f"Fase A · listados: {len(destination_codes)} destinos × {pages_per_destination} páginas")
    listing_futures = [
        (fetch_listing_page.submit(code, page, checkin, checkout), f"listado {code}·p{page}")
        for code in destination_codes
        for page in range(1, pages_per_destination + 1)
    ]
    listings, listing_failures = collect(listing_futures, "páginas de listado", artifact_key="hoteles-listados")

    by_destination: dict[str, list[dict]] = {code: [] for code in destination_codes}
    for page in listings:
        by_destination[page["destination_code"]].append(page)

    logger.info(f"Fase B · detalle: top {details_per_destination} hoteles por destino (más reseñas) para obtener tipos de habitación")
    detail_futures = []
    for code, pages in by_destination.items():
        hotels = [h for p in pages for h in p["hotels"] if h.get("booking_url")]
        hotels.sort(key=lambda h: (h.get("rating") or {}).get("reviews") or 0, reverse=True)
        for h in hotels[:details_per_destination]:
            detail_futures.append((fetch_hotel_detail.submit(code, h["name"], h["booking_url"], checkin, checkout), f"detalle {code}·{h['name'][:30]}"))
    details, detail_failures = collect(detail_futures, "detalles de hotel", artifact_key="hoteles-detalles")

    extraction_table("hoteles-extraccion", "Resultado de cada petición a TiquetesBaratos", listings + details, listing_failures + detail_failures, "recurso")
    return handoff.put("hoteles-extraidos", {
        "by_destination": by_destination, "details": details, "failures": listing_failures + detail_failures,
        "requests": len(listing_futures) + len(detail_futures),
    })


@flow(name="hoteles-2-transformacion", flow_run_name="hoteles·transformacion", task_runner=dask_runner(), log_prints=True)
def transform_hotels(extraction_ref: str, run_id: str | None, rate_start: str, rate_days: int) -> str:
    logger = get_run_logger()
    extraction = handoff.take(extraction_ref)
    details_by_dest: dict[str, list[dict]] = {}
    for d in extraction["details"]:
        details_by_dest.setdefault(d["recurso"].split("·")[0], []).append(d)

    clean_futures = [
        (clean_destination.submit(code, pages, details_by_dest.get(code, []), run_id), f"limpieza {code}")
        for code, pages in extraction["by_destination"].items()
        if pages
    ]
    cleaned, clean_failures = collect(clean_futures, "limpieza por destino", artifact_key="hoteles-limpieza")
    hotels = [row for c in cleaned for row in c["rows"]]
    logger.info(f"{len(hotels)} hoteles limpios → motor de tarifas para {rate_days} noches desde {rate_start}")

    rates_result = compute_hotel_rates.submit(hotels, rate_start, rate_days).result()
    create_table_artifact(
        key="hoteles-particiones-dask",
        table=[{"particion": i, **p} for i, p in enumerate(rates_result["partitions"])],
        description="Particiones del Dask Bag del motor de tarifas y el worker que procesó cada una",
    )
    return handoff.put("hoteles-transformados", {
        "hotels": hotels, "rates": rates_result["rates"], "by_worker": rates_result.get("by_worker", {}),
        "discarded": {c["destination_code"]: c["discarded"] for c in cleaned}, "failures": clean_failures,
    })


@flow(name="hoteles-3-carga", flow_run_name="hoteles·carga", task_runner=dask_runner(), log_prints=True)
def load_hotels(dataset_ref: str) -> dict[str, Any]:
    data = handoff.take(dataset_ref)
    hotels, rates = data["hotels"], data["rates"]
    # Primero hoteles (las tarifas tienen FK a hotels.id), luego las tarifas en paralelo.
    hotel_load = load_table("hotels", hotels, batch_size=200)
    rate_load = load_table("hotel_rates", rates, batch_size=500)
    return {"hotels": hotel_load, "hotel_rates": rate_load}


# ----------------------------------------------------------------------------- flow principal
@flow(name="ingesta-hoteles", flow_run_name="ingesta-hoteles·checkin+{checkin_offset_days}d", log_prints=True)
def ingest_hotels(
    destination_codes: list[str] | None = None,
    pages_per_destination: int = 2,
    details_per_destination: int = 4,
    checkin_offset_days: int = 30,
    nights: int = 3,
    rate_days: int = 14,
    ingestion_run_id: str | None = None,
) -> dict[str, Any]:
    codes = destination_codes or list(DESTINATIONS)
    checkin = date.today() + timedelta(days=checkin_offset_days)
    checkout = checkin + timedelta(days=nights)
    own_run = ingestion_run_id is None
    run_id = register_run_start("ingesta-hoteles") if own_run else ingestion_run_id

    extraction_ref = extract_hotels(codes, pages_per_destination, details_per_destination, checkin.isoformat(), checkout.isoformat())
    extraction = {k: v for k, v in handoff.peek(extraction_ref).items() if k in ("failures", "requests")}
    transformed_ref = transform_hotels(extraction_ref, run_id, date.today().isoformat(), rate_days)
    transformed = {k: v for k, v in handoff.peek(transformed_ref).items()}
    loaded = load_hotels(transformed_ref)

    failures = extraction["failures"] + transformed["failures"] + loaded["hotels"]["failed"] + loaded["hotel_rates"]["failed"]
    stats = {
        "peticiones": extraction["requests"],
        "fallos_definitivos": len(failures),
        "hoteles": len(transformed["hotels"]),
        "tarifas": len(transformed["rates"]),
        "tarifas_por_worker": transformed["by_worker"],
        "descartados": transformed["discarded"],
    }
    create_markdown_artifact(
        key="hoteles-resumen",
        markdown=(
            "## Ingesta de hoteles\n"
            f"- Fuente: **TiquetesBaratos /ai/** (real) · destinos: {', '.join(codes)}\n"
            f"- Peticiones: **{stats['peticiones']}** · fallos definitivos: **{stats['fallos_definitivos']}**\n"
            f"- Hoteles limpios: **{stats['hoteles']}** · tarifas calculadas: **{stats['tarifas']}**\n"
            f"- Tarifas (particiones) por worker Dask: `{stats['tarifas_por_worker']}`\n"
            f"- Descartados por destino: `{stats['descartados']}`\n"
        ),
        description="Resumen de la ingesta de hoteles",
    )
    if own_run:
        register_run_finish(run_id, "completed_with_errors" if failures else "completed", stats)
    return stats
