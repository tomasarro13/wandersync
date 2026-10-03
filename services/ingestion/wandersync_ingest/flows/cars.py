"""Flow de autos: proveedor de alquiler (mock con caos) → normalización → Supabase."""

from __future__ import annotations

from datetime import date, timedelta
from typing import Any

from prefect import flow, task
from prefect.artifacts import create_markdown_artifact
from prefect.cache_policies import NO_CACHE

from ..config import DEFAULT_CAR_LOCATIONS
from ..runner import TRANSIENT_RETRY, dask_runner
from ..sources.mock_providers import MockCarRentalClient
from ..transform.cars import CarNormalizer
from . import handoff
from .common import collect, extraction_table, load_table, register_run_finish, register_run_start, with_run_meta


@task(
    name="MockCarRentalClient.search_availability",
    task_run_name="MockCarRentalClient.search_availability[{location}·{pickup_date}]",
    tags=["mock-rentals", "extraccion"],
    timeout_seconds=60,
    cache_policy=NO_CACHE,
    **TRANSIENT_RETRY,
)
def search_cars(location: str, pickup_date: str) -> dict[str, Any]:
    with MockCarRentalClient() as client:
        result = client.search_availability(location, pickup_date)
    result["recurso"] = f"rentals {location} {pickup_date}"
    return with_run_meta(result)


@task(name="CarNormalizer.normalize_batch", task_run_name="CarNormalizer.normalize_batch[{n_results}resultados]",
      tags=["transformacion"], cache_policy=NO_CACHE)
def normalize_cars(results: list[dict], run_id: str | None, n_results: int) -> dict[str, Any]:
    return with_run_meta(CarNormalizer(run_id).normalize_batch(results))


@flow(name="autos-1-extraccion", flow_run_name="autos·extraccion", task_runner=dask_runner(), log_prints=True)
def extract_cars(locations: list[str], dates: list[str]) -> str:
    futures = [(search_cars.submit(loc, day), f"rentals {loc} {day}") for loc in locations for day in dates]
    ok, failed = collect(futures, "disponibilidad de autos", artifact_key="autos-busquedas")
    extraction_table("autos-extraccion", "Resultado de cada consulta al proveedor de autos", ok, failed, "recurso")
    return handoff.put("autos-extraidos", {"results": ok, "failures": failed, "requests": len(futures)})


@flow(name="autos-2-transformacion-y-carga", flow_run_name="autos·transformacion-carga", task_runner=dask_runner(), log_prints=True)
def transform_load_cars(dataset_ref: str, run_id: str | None) -> dict[str, Any]:
    results = handoff.take(dataset_ref)["results"]
    normalized = normalize_cars.submit(results, run_id, len(results)).result()
    loaded = load_table("cars", normalized["rows"], batch_size=300)
    return {"cars": normalized["rows"], "discarded": normalized["discarded"], "load": loaded}


@flow(name="ingesta-autos", flow_run_name="ingesta-autos·{days_ahead}", log_prints=True)
def ingest_cars(locations: list[str] | None = None, days_ahead: list[int] = [7, 14, 30], ingestion_run_id: str | None = None) -> dict[str, Any]:
    locations = locations or DEFAULT_CAR_LOCATIONS
    dates = [(date.today() + timedelta(days=d)).isoformat() for d in days_ahead]
    own_run = ingestion_run_id is None
    run_id = register_run_start("ingesta-autos") if own_run else ingestion_run_id

    extraction_ref = extract_cars(locations, dates)
    extraction = {k: v for k, v in handoff.peek(extraction_ref).items() if k in ("failures", "requests")}
    result = transform_load_cars(extraction_ref, run_id)

    failures = extraction["failures"] + result["load"]["failed"]
    stats = {
        "peticiones": extraction["requests"],
        "fallos_definitivos": len(failures),
        "recursos_fallidos": [f["recurso"] for f in extraction["failures"]],
        "autos": len(result["cars"]),
        "descartados": result["discarded"],
    }
    create_markdown_artifact(
        key="autos-resumen",
        markdown=(
            "## Ingesta de autos\n"
            f"- Peticiones: **{stats['peticiones']}** · fallos definitivos: **{stats['fallos_definitivos']}** {stats['recursos_fallidos']}\n"
            f"- Vehículos válidos: **{stats['autos']}** · descartados: `{stats['descartados']}`\n"
        ),
        description="Resumen de la ingesta de autos",
    )
    if own_run:
        register_run_finish(run_id, "completed_with_errors" if failures else "completed", stats)
    return stats
