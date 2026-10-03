"""Piezas compartidas por los flows: seguimiento de futures, carga por lotes y registro de runs."""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Iterable

from distributed import as_completed as dask_as_completed
from prefect import get_run_logger, task
from prefect.artifacts import create_progress_artifact, create_table_artifact, update_progress_artifact
from prefect.cache_policies import NO_CACHE
from prefect.client.orchestration import get_client
from prefect.futures import PrefectFuture, as_completed
from prefect.runtime import flow_run, task_run

from ..persistence.supabase import SupabaseRepository
from ..runner import TRANSIENT_RETRY
from ..tracing import worker_name


def with_run_meta(result: dict[str, Any]) -> dict[str, Any]:
    """Agrega al resultado de una task el worker de Dask que la ejecutó y el número de intentos."""
    result["worker"] = worker_name()
    result["attempts"] = task_run.run_count or 1
    return result


def _attempts_of(task_run_id) -> int | None:
    try:
        with get_client(sync_client=True) as client:
            return client.read_task_run(task_run_id).run_count
    except Exception:
        return None


def _as_completed(futures: list[PrefectFuture]) -> Iterable[PrefectFuture]:
    """`as_completed` de Prefect se dispara con el primer estado Failed aunque la task vaya a
    reintentar. Con Dask usamos la señal del propio future de Dask, que solo se completa cuando
    la task terminó de verdad (incluidos todos sus reintentos)."""
    wrapped = {getattr(f, "wrapped_future", None): f for f in futures}
    if None in wrapped:
        yield from as_completed(futures)
        return
    client = next(iter(wrapped)).client  # el runner crea su cliente con set_as_default=False
    with client.as_current():
        for dask_future in dask_as_completed(list(wrapped)):
            yield wrapped[dask_future]


def collect(futures: list[tuple[PrefectFuture, str]], label: str, artifact_key: str) -> tuple[list[dict], list[dict]]:
    """Espera los futures a medida que terminan (no en orden de envío) y lo reporta en vivo:
    una línea de log por cada task finalizada + un artifact de progreso que se actualiza."""
    logger = get_run_logger()
    total = len(futures)
    if total == 0:
        return [], []
    names = {f.task_run_id: name for f, name in futures}
    progress_id = create_progress_artifact(progress=0.0, key=artifact_key, description=f"Progreso en vivo: {label}")
    logger.info(f"⏳ {label}: {total} tareas enviadas al clúster Dask, esperando resultados…")

    ok: list[dict] = []
    failed: list[dict] = []
    for i, future in enumerate(_as_completed([f for f, _ in futures]), start=1):
        name = names[future.task_run_id]
        future.wait()
        state = future.state
        if state.is_completed():
            result = future.result()
            ok.append(result)
            logger.info(f"✔ [{i}/{total}] {name} → {result.get('summary')} · {result.get('worker')} · intentos={result.get('attempts')}")
        else:
            attempts = _attempts_of(future.task_run_id)
            failed.append({"recurso": name, "estado": state.name, "intentos": attempts, "error": (state.message or "")[:200]})
            logger.warning(f"✖ [{i}/{total}] {name} → {state.name} tras {attempts} intentos · {state.message}")
        update_progress_artifact(progress_id, progress=round(100 * i / total, 1))

    logger.info(f"🏁 {label}: {len(ok)} correctas · {len(failed)} fallidas definitivamente")
    return ok, failed


def extraction_table(key: str, title: str, ok: list[dict], failed: list[dict], resource_field: str) -> None:
    rows = [
        {"recurso": r.get(resource_field, r.get("summary")), "estado": "Completed", "intentos": r.get("attempts"),
         "worker": r.get("worker"), "resultado": r.get("summary")}
        for r in ok
    ] + [
        {"recurso": f["recurso"], "estado": f["estado"], "intentos": f["intentos"], "worker": "-", "resultado": f["error"]}
        for f in failed
    ]
    create_table_artifact(key=key, table=rows, description=title)


def batches(rows: list[dict], size: int) -> Iterable[tuple[int, list[dict]]]:
    for start in range(0, len(rows), size):
        yield start // size + 1, rows[start:start + size]


@task(
    name="SupabaseRepository.upsert",
    task_run_name="SupabaseRepository.upsert[{table}·lote{batch_no}]",
    tags=["supabase", "carga"],
    cache_policy=NO_CACHE,
    **TRANSIENT_RETRY,
)
def upsert_batch(table: str, batch_no: int, rows: list[dict], on_conflict: str = "id") -> dict[str, Any]:
    repo = SupabaseRepository()
    try:
        result = repo.upsert(table, rows, on_conflict=on_conflict)
    finally:
        repo.close()
    result["lote"] = f"{table}·lote{batch_no}"
    return with_run_meta(result)


def load_table(table: str, rows: list[dict], batch_size: int = 400, on_conflict: str = "id") -> dict[str, Any]:
    """Envía todos los lotes de una tabla en paralelo al clúster y espera su confirmación."""
    futures = [(upsert_batch.submit(table, n, chunk, on_conflict), f"{table}·lote{n}") for n, chunk in batches(rows, batch_size)]
    ok, failed = collect(futures, f"carga de {table}", artifact_key=f"carga-{table.replace('_', '-')}")
    return {"table": table, "rows": sum(r["rows"] for r in ok), "batches_ok": len(ok), "batches_failed": len(failed), "failed": failed}


@task(name="RunRegistry.start", task_run_name="RunRegistry.start[{flow_name}]", cache_policy=NO_CACHE, retries=2, retry_delay_seconds=3)
def register_run_start(flow_name: str) -> str | None:
    repo = SupabaseRepository()
    try:
        row = repo.insert_returning("ingestion_runs", {"flow_name": flow_name, "flow_run_id": str(flow_run.id), "status": "running"})
    finally:
        repo.close()
    run_id = row["id"] if row else None
    get_run_logger().info(f"🆔 ingestion_run_id={run_id or 'dry-run'} · flow_run_id={flow_run.id}")
    return run_id


@task(name="RunRegistry.finish", task_run_name="RunRegistry.finish[{status}]", cache_policy=NO_CACHE, retries=2, retry_delay_seconds=3)
def register_run_finish(run_id: str | None, status: str, stats: dict[str, Any]) -> None:
    if not run_id:
        return
    repo = SupabaseRepository()
    try:
        repo.update("ingestion_runs", {"id": run_id}, {"status": status, "stats": stats, "finished_at": datetime.now(timezone.utc).isoformat()})
    finally:
        repo.close()
