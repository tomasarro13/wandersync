"""Flow maestro: orquesta las tres ingestas como subflows y deja un resumen consolidado."""

from __future__ import annotations

from typing import Any

from prefect import flow, get_run_logger
from prefect.artifacts import create_markdown_artifact

from .cars import ingest_cars
from .common import register_run_finish, register_run_start
from .flights import ingest_flights
from .hotels import ingest_hotels


@flow(name="wandersync-ingesta-maestra", flow_run_name="maestra·{run_label}", log_prints=True)
def master_ingestion(run_label: str = "manual", include_hotels: bool = True, include_flights: bool = True, include_cars: bool = True) -> dict[str, Any]:
    logger = get_run_logger()
    run_id = register_run_start("wandersync-ingesta-maestra")
    results: dict[str, Any] = {}

    # Cada dominio es independiente: si uno revienta, los demás igual se ejecutan.
    for name, enabled, fn in (
        ("vuelos", include_flights, lambda: ingest_flights(ingestion_run_id=run_id)),
        ("hoteles", include_hotels, lambda: ingest_hotels(ingestion_run_id=run_id)),
        ("autos", include_cars, lambda: ingest_cars(ingestion_run_id=run_id)),
    ):
        if not enabled:
            continue
        logger.info(f"──────── iniciando subflow de {name} ────────")
        try:
            results[name] = fn()
        except Exception as exc:
            logger.error(f"El subflow de {name} falló: {type(exc).__name__}: {exc}")
            results[name] = {"error": f"{type(exc).__name__}: {exc}", "fallos_definitivos": 1}

    total_failures = sum(r.get("fallos_definitivos", 0) for r in results.values())
    status = "completed_with_errors" if total_failures else "completed"
    lines = ["# Ingesta WanderSync", "", "| Dominio | Peticiones | Registros | Fallos definitivos |", "|---|---|---|---|"]
    for name, r in results.items():
        records = r.get("vuelos", r.get("hoteles", r.get("autos", "-")))
        lines.append(f"| {name} | {r.get('peticiones', '-')} | {records} | {r.get('fallos_definitivos', '-')} |")
    lines += ["", f"Estado final: **{status}**"]
    create_markdown_artifact(key="ingesta-maestra-resumen", markdown="\n".join(lines), description="Resumen consolidado de la ingesta")

    register_run_finish(run_id, status, results)
    return results
