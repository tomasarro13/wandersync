"""Flow de vuelos: feed de aerolíneas (mock con caos) + Duffel test → normalización → estadísticas (Dask DataFrame) → Supabase."""

from __future__ import annotations

from datetime import date, timedelta
from typing import Any

from prefect import flow, get_run_logger, task
from prefect.artifacts import create_markdown_artifact, create_table_artifact
from prefect.cache_policies import NO_CACHE
from prefect_dask import get_dask_client

from ..config import DEFAULT_DUFFEL_ROUTES, DEFAULT_ROUTES, settings
from ..runner import TRANSIENT_RETRY, dask_runner
from ..sources.duffel import DuffelFlightClient
from ..sources.mock_providers import MockAirlineFeedClient
from ..transform.flights import FlightNormalizer, RouteStatsCalculator
from . import handoff
from .common import collect, extraction_table, load_table, register_run_finish, register_run_start, with_run_meta


@task(
    name="MockAirlineFeedClient.search_itineraries",
    task_run_name="MockAirlineFeedClient.search_itineraries[{origin}→{destination}·{date}]",
    tags=["mock-airlines", "extraccion"],
    timeout_seconds=60,
    cache_policy=NO_CACHE,
    **TRANSIENT_RETRY,
)
def search_mock_itineraries(origin: str, destination: str, date: str) -> dict[str, Any]:
    with MockAirlineFeedClient() as client:
        result = client.search_itineraries(origin, destination, date)
    result["recurso"] = f"airlines {origin}→{destination} {date}"
    return with_run_meta(result)


@task(
    name="DuffelFlightClient.search_offers",
    task_run_name="DuffelFlightClient.search_offers[{origin}→{destination}·{date}]",
    tags=["duffel", "extraccion"],
    timeout_seconds=90,
    cache_policy=NO_CACHE,
    **TRANSIENT_RETRY,
)
def search_duffel_offers(origin: str, destination: str, date: str) -> dict[str, Any]:
    with DuffelFlightClient() as client:
        result = client.search_offers(origin, destination, date)
    result["recurso"] = f"duffel {origin}→{destination} {date}"
    return with_run_meta(result)


@task(name="FlightNormalizer.normalize_batch", task_run_name="FlightNormalizer.normalize_batch[{n_results}resultados]",
      tags=["transformacion"], cache_policy=NO_CACHE)
def normalize_flights(mock_results: list[dict], duffel_results: list[dict], run_id: str | None, n_results: int) -> dict[str, Any]:
    return with_run_meta(FlightNormalizer(run_id).normalize_batch(mock_results, duffel_results))


@task(name="RouteStatsCalculator.compute", task_run_name="RouteStatsCalculator.compute[{n_rows}vuelos]",
      tags=["transformacion", "dask-dataframe"], cache_policy=NO_CACHE)
def compute_route_stats(rows: list[dict], n_rows: int) -> dict[str, Any]:
    with get_dask_client() as client:
        result = RouteStatsCalculator().compute(client, rows)
    return with_run_meta(result)


@flow(name="vuelos-1-extraccion", flow_run_name="vuelos·extraccion", task_runner=dask_runner(), log_prints=True)
def extract_flights(routes: list[str], duffel_routes: list[str], dates: list[str]) -> str:
    logger = get_run_logger()
    futures = []
    for route in routes:
        o, d = route.split("-")
        for day in dates:
            futures.append((search_mock_itineraries.submit(o, d, day), f"airlines {route} {day}"))
    if settings.duffel_enabled:
        for route in duffel_routes:
            o, d = route.split("-")
            for day in dates:
                futures.append((search_duffel_offers.submit(o, d, day), f"duffel {route} {day}"))
    else:
        logger.warning("DUFFEL_ACCESS_TOKEN no configurado: se omite la fuente Duffel en esta ejecución")

    ok, failed = collect(futures, "búsquedas de vuelos", artifact_key="vuelos-busquedas")
    extraction_table("vuelos-extraccion", "Resultado de cada búsqueda de vuelos (feed + Duffel)", ok, failed, "recurso")
    return handoff.put("vuelos-extraidos", {
        "mock": [r for r in ok if "itineraries" in r],
        "duffel": [r for r in ok if "offers" in r],
        "failures": failed,
        "requests": len(futures),
    })


@flow(name="vuelos-2-transformacion", flow_run_name="vuelos·transformacion", task_runner=dask_runner(), log_prints=True)
def transform_flights(extraction_ref: str, run_id: str | None) -> str:
    extraction = handoff.take(extraction_ref)
    n = len(extraction["mock"]) + len(extraction["duffel"])
    normalized = normalize_flights.submit(extraction["mock"], extraction["duffel"], run_id, n).result()
    stats = compute_route_stats.submit(normalized["rows"], len(normalized["rows"])).result()
    create_table_artifact(key="vuelos-rutas", table=stats["rows"], description="Precio mínimo/promedio/máximo por ruta (Dask DataFrame groupby)")
    return handoff.put("vuelos-transformados", {"flights": normalized["rows"], "route_stats": stats["rows"], "discarded": normalized["discarded"]})


@flow(name="vuelos-3-carga", flow_run_name="vuelos·carga", task_runner=dask_runner(), log_prints=True)
def load_flights(dataset_ref: str) -> dict[str, Any]:
    data = handoff.take(dataset_ref)
    flights, route_stats = data["flights"], data["route_stats"]
    return {"flights": load_table("flights", flights, batch_size=300), "route_price_stats": load_table("route_price_stats", route_stats, on_conflict="route")}


@flow(name="ingesta-vuelos", flow_run_name="ingesta-vuelos·{days_ahead}", log_prints=True)
def ingest_flights(
    routes: list[str] | None = None,
    duffel_routes: list[str] | None = None,
    days_ahead: list[int] = [7, 14, 30],
    ingestion_run_id: str | None = None,
) -> dict[str, Any]:
    routes = routes or DEFAULT_ROUTES
    duffel_routes = duffel_routes or DEFAULT_DUFFEL_ROUTES
    dates = [(date.today() + timedelta(days=d)).isoformat() for d in days_ahead]
    own_run = ingestion_run_id is None
    run_id = register_run_start("ingesta-vuelos") if own_run else ingestion_run_id

    extraction_ref = extract_flights(routes, duffel_routes, dates)
    extraction = {k: v for k, v in handoff.peek(extraction_ref).items() if k in ("failures", "requests")}
    transformed_ref = transform_flights(extraction_ref, run_id)
    transformed = dict(handoff.peek(transformed_ref))
    loaded = load_flights(transformed_ref)

    failures = extraction["failures"] + loaded["flights"]["failed"] + loaded["route_price_stats"]["failed"]
    stats = {
        "peticiones": extraction["requests"],
        "fallos_definitivos": len(failures),
        "recursos_fallidos": [f["recurso"] for f in extraction["failures"]],
        "vuelos": len(transformed["flights"]),
        "por_fuente": {
            "mock-airlines": sum(1 for f in transformed["flights"] if f["source"] == "mock-airlines"),
            "duffel": sum(1 for f in transformed["flights"] if f["source"] == "duffel"),
        },
        "rutas": len(transformed["route_stats"]),
        "descartados": transformed["discarded"],
    }
    create_markdown_artifact(
        key="vuelos-resumen",
        markdown=(
            "## Ingesta de vuelos\n"
            f"- Peticiones: **{stats['peticiones']}** · fallos definitivos: **{stats['fallos_definitivos']}** {stats['recursos_fallidos']}\n"
            f"- Vuelos válidos: **{stats['vuelos']}** · por fuente: `{stats['por_fuente']}`\n"
            f"- Rutas agregadas: **{stats['rutas']}** · descartados: `{stats['descartados']}`\n"
        ),
        description="Resumen de la ingesta de vuelos",
    )
    if own_run:
        register_run_finish(run_id, "completed_with_errors" if failures else "completed", stats)
    return stats
