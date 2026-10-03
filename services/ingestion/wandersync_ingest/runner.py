"""Task runner de Dask con nombres legibles + políticas de reintento compartidas."""

from __future__ import annotations

import re
import uuid
from typing import Any

from prefect.tasks import exponential_backoff
from prefect_dask import DaskTaskRunner
from prefect_dask.task_runners import PrefectDaskFuture

from .config import settings
from .sources.errors import TransientSourceError

_SAFE = re.compile(r"[^A-Za-z0-9_.·→]+")


def _label(parameters: dict[str, Any]) -> str:
    parts = [str(v) for v in parameters.values() if isinstance(v, (str, int, float)) and len(str(v)) <= 24]
    return _SAFE.sub("_", "·".join(parts))[:60] or "run"


class NamedDaskTaskRunner(DaskTaskRunner):
    """DaskTaskRunner que asigna a cada tarea una *key* de Dask descriptiva.

    Formato: ``<Clase.metodo>-<8 hex>-<parámetros>``
    El dashboard de Dask agrupa por el prefijo antes del primer bloque hexadecimal, así que
    la vista *Progress* muestra una barra por método (p. ej. ``TiquetesBaratosHotelScraper.fetch_listing_page``)
    y al pasar el mouse por el *Task Stream* se ve la key completa con ciudad/página/ruta.
    """

    def submit(self, task, parameters, wait_for=None, dependencies=None):  # type: ignore[override]
        if not self._started:
            raise RuntimeError("The task runner must be started before submitting work.")
        parameters = self._optimize_futures(parameters)
        wait_for = self._optimize_futures(wait_for) if wait_for else None
        key = f"{task.name}-{uuid.uuid4().hex[:8]}-{_label(parameters)}"
        future = self.client.submit(
            task,
            key=key,
            parameters=parameters,
            wait_for=wait_for,
            dependencies=dependencies,
            return_type="state",
        )
        return PrefectDaskFuture(wrapped_future=future, task_run_id=future.task_run_id)


def dask_runner() -> NamedDaskTaskRunner:
    return NamedDaskTaskRunner(address=settings.dask_scheduler_address)


def retry_only_transient(task, task_run, state) -> bool:
    """Reintenta solo errores transitorios (red, 429, 5xx, timeout, payload corrupto).
    Un 400/404 no se arregla reintentando: falla de inmediato."""
    try:
        state.result()
    except TransientSourceError:
        return True
    except Exception:
        return False
    return False


# 3 reintentos con backoff exponencial (2s, 4s, 8s) ± 50 % de jitter.
TRANSIENT_RETRY: dict[str, Any] = {
    "retries": 3,
    "retry_delay_seconds": exponential_backoff(backoff_factor=2),
    "retry_jitter_factor": 0.5,
    "retry_condition_fn": retry_only_transient,
}
