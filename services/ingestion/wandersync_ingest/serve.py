"""Proceso de larga vida del contenedor `ingestion`.

1. Crea los límites de concurrencia por tag (las tasks esperan turno en estado
   "AwaitingConcurrencySlot", visible en la UI).
2. Registra los deployments en el servidor Prefect y queda escuchando ejecuciones:
   programadas (cron) o manuales desde la UI (botón "Run").
"""

from __future__ import annotations

import logging
import os
import threading
import time

from prefect import serve
from prefect.client.orchestration import get_client
from prefect.deployments import run_deployment
from prefect.deployments.runner import EntrypointType

from .flows.cars import ingest_cars
from .flows.flights import ingest_flights
from .flows.hotels import ingest_hotels
from .flows.master import master_ingestion

log = logging.getLogger("wandersync.serve")
logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)-7s | %(message)s")

CONCURRENCY_LIMITS = {
    "tiquetesbaratos": int(os.getenv("LIMIT_TIQUETESBARATOS", "2")),  # fuente real: ser amables
    "duffel": int(os.getenv("LIMIT_DUFFEL", "3")),
    "supabase": int(os.getenv("LIMIT_SUPABASE", "4")),
}


def ensure_concurrency_limits() -> None:
    with get_client(sync_client=True) as client:
        for tag, limit in CONCURRENCY_LIMITS.items():
            client.create_concurrency_limit(tag=tag, concurrency_limit=limit)
            log.info(f"límite de concurrencia: tag '{tag}' → {limit} tareas simultáneas")


def trigger_on_start() -> None:
    time.sleep(15)  # dar tiempo a que serve() registre los deployments
    run = run_deployment("wandersync-ingesta-maestra/demo-manual", parameters={"run_label": "arranque"}, timeout=0)
    log.info(f"ejecución inicial disparada: {run.name}")


def main() -> None:
    ensure_concurrency_limits()
    schedule_enabled = os.getenv("INGESTION_SCHEDULE_ENABLED", "true").lower() == "true"
    cron = os.getenv("INGESTION_CRON", "0 * * * *")

    deployments = [
        master_ingestion.to_deployment(
            name="programada",
            cron=cron if schedule_enabled else None,
            parameters={"run_label": "programada"},
            description="Ingesta completa periódica (vuelos, hoteles, autos)",
            tags=["wandersync", "programada"],
            entrypoint_type=EntrypointType.MODULE_PATH,
        ),
        master_ingestion.to_deployment(
            name="demo-manual",
            parameters={"run_label": "demo"},
            description="Ingesta completa bajo demanda (para la demostración)",
            tags=["wandersync", "demo"],
            entrypoint_type=EntrypointType.MODULE_PATH,
        ),
        ingest_hotels.to_deployment(name="manual", tags=["wandersync", "hoteles"], entrypoint_type=EntrypointType.MODULE_PATH),
        ingest_flights.to_deployment(name="manual", tags=["wandersync", "vuelos"], entrypoint_type=EntrypointType.MODULE_PATH),
        ingest_cars.to_deployment(name="manual", tags=["wandersync", "autos"], entrypoint_type=EntrypointType.MODULE_PATH),
    ]

    if os.getenv("RUN_INGESTION_ON_START", "false").lower() == "true":
        threading.Thread(target=trigger_on_start, daemon=True).start()

    log.info(f"sirviendo {len(deployments)} deployments · programación={'ON ' + cron if schedule_enabled else 'OFF'}")
    serve(*deployments, limit=int(os.getenv("SERVE_MAX_CONCURRENT_RUNS", "2")))


if __name__ == "__main__":
    main()
