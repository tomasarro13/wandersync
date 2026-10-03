"""Trazado explícito de funciones y métodos.

Prefect por sí solo muestra cuándo una *task* empieza y termina. El requisito del proyecto
es más fino: ver en la UI **el momento en que se invoca cada función o método de clase**,
con sus argumentos, el worker de Dask que la ejecuta, el intento de reintento y su duración.

`@traced` envuelve cualquier función/método y, en cada llamada:
  1. escribe en los logs de Prefect  ▶ ENTRA  Clase.metodo(args) · worker · intento
  2. emite un evento Prefect  wandersync.fn.started   (pestaña Events)
  3. al terminar escribe       ◀ SALE   Clase.metodo → resumen del resultado · duración
     o, si falla,              ✖ ERROR  Clase.metodo · Excepción · duración
     y emite wandersync.fn.completed / wandersync.fn.failed

Si la función corre fuera de un contexto de Prefect (p. ej. una partición de Dask Bag
ejecutándose directamente en un worker) los mensajes van al logger estándar del worker,
visibles en `docker compose logs dask-worker-N` y en el dashboard de Dask (Info → Logs).
"""

from __future__ import annotations

import functools
import inspect
import logging
import os
import socket
import time
import uuid
from typing import Any, Callable, TypeVar

from .config import settings

F = TypeVar("F", bound=Callable[..., Any])

_fallback_logger = logging.getLogger("wandersync")
if not _fallback_logger.handlers:
    _handler = logging.StreamHandler()
    _handler.setFormatter(logging.Formatter("%(asctime)s | %(levelname)-7s | %(message)s"))
    _fallback_logger.addHandler(_handler)
    _fallback_logger.setLevel(logging.INFO)


def current_logger() -> logging.Logger | logging.LoggerAdapter:
    """Logger de Prefect si hay un run activo; si no, el logger estándar."""
    try:
        from prefect import get_run_logger

        return get_run_logger()
    except Exception:
        return _fallback_logger


def worker_name() -> str:
    """Nombre del worker de Dask que ejecuta el código (o el host si no hay worker)."""
    try:
        from distributed import get_worker

        w = get_worker()
        return str(w.name or w.address)
    except Exception:
        return os.getenv("HOSTNAME", socket.gethostname())


def attempt_info() -> str:
    """'intento 2/4' cuando estamos dentro de una task con reintentos."""
    try:
        from prefect.context import TaskRunContext

        ctx = TaskRunContext.get()
        if ctx is None:
            return ""
        run_count = max(ctx.task_run.run_count, 1)
        total = (ctx.task.retries or 0) + 1
        return f"intento {run_count}/{total}"
    except Exception:
        return ""


def _short(value: Any, limit: int = 60) -> str:
    if isinstance(value, (bytes, bytearray)):
        return f"<{len(value) / 1024:.1f}KB>"
    if isinstance(value, str):
        return repr(value) if len(value) <= limit else f"<texto {len(value) / 1024:.1f}KB>"
    if isinstance(value, dict):
        return f"<dict {len(value)} claves>"
    if isinstance(value, (list, tuple, set)):
        return f"<{type(value).__name__} {len(value)} elementos>"
    text = repr(value)
    return text if len(text) <= limit else text[: limit - 1] + "…"


def summarize_args(fn: Callable, args: tuple, kwargs: dict) -> str:
    try:
        bound = inspect.signature(fn).bind_partial(*args, **kwargs)
        items = [(k, v) for k, v in bound.arguments.items() if k not in ("self", "cls")]
    except (TypeError, ValueError):
        items = [(f"arg{i}", a) for i, a in enumerate(args)] + list(kwargs.items())
    return ", ".join(f"{k}={_short(v)}" for k, v in items)


def summarize_result(result: Any) -> str:
    if result is None:
        return "None"
    if isinstance(result, dict) and "summary" in result:
        return str(result["summary"])
    return _short(result, 80)


def _emit(event: str, qualname: str, payload: dict[str, Any]) -> None:
    if not settings.trace_events:
        return
    try:
        from prefect.events import emit_event

        emit_event(
            event=event,
            resource={
                "prefect.resource.id": f"wandersync.fn.{qualname}",
                "prefect.resource.name": qualname,
            },
            payload=payload,
        )
    except Exception:
        pass  # la observabilidad nunca debe tumbar la ingesta


def traced(fn: F | None = None, *, name: str | None = None) -> F | Callable[[F], F]:
    """Decorador de trazado. Uso: `@traced` o `@traced(name="Alias")`."""

    def decorator(func: F) -> F:
        qualname = name or func.__qualname__

        @functools.wraps(func)
        def wrapper(*args: Any, **kwargs: Any) -> Any:
            logger = current_logger()
            call_id = uuid.uuid4().hex[:6]
            worker = worker_name()
            attempt = attempt_info()
            arg_text = summarize_args(func, args, kwargs)
            context = " · ".join(p for p in (f"worker={worker}", attempt, f"call={call_id}") if p)

            logger.info(f"▶ ENTRA  {qualname}({arg_text}) · {context}")
            _emit("wandersync.fn.started", qualname, {"call_id": call_id, "worker": worker, "args": arg_text, "attempt": attempt})

            started = time.perf_counter()
            try:
                result = func(*args, **kwargs)
            except Exception as exc:
                elapsed = time.perf_counter() - started
                logger.error(f"✖ ERROR  {qualname} · {type(exc).__name__}: {exc} · {elapsed:.2f}s · {context}")
                _emit("wandersync.fn.failed", qualname, {"call_id": call_id, "worker": worker, "error": f"{type(exc).__name__}: {exc}", "seconds": round(elapsed, 3)})
                raise
            elapsed = time.perf_counter() - started
            summary = summarize_result(result)
            logger.info(f"◀ SALE   {qualname} → {summary} · {elapsed:.2f}s · call={call_id}")
            _emit("wandersync.fn.completed", qualname, {"call_id": call_id, "worker": worker, "result": summary, "seconds": round(elapsed, 3)})
            return result

        return wrapper  # type: ignore[return-value]

    if fn is not None:
        return decorator(fn)
    return decorator


def step(message: str) -> None:
    """Paso intermedio dentro de una función trazada (sub-actividad)."""
    current_logger().info(f"   • {message}")
