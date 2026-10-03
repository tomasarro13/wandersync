"""Paso de datasets entre subflows por referencia.

Prefect guarda los parámetros de cada flow run en su base de datos y los limita a 512 KB.
Las etapas (extracción → transformación → carga) se intercambian varios MB, así que en vez
del dataset se pasa una referencia corta ("ds-…"), que además se ve legible en la UI.
Los subflows se ejecutan en el mismo proceso que el flow padre, por eso basta memoria local.
"""

from __future__ import annotations

import uuid
from typing import Any

_store: dict[str, Any] = {}


def put(label: str, data: Any) -> str:
    ref = f"ds-{label}-{uuid.uuid4().hex[:8]}"
    _store[ref] = data
    return ref


def take(ref: str) -> Any:
    """Recupera y libera el dataset (cada referencia se consume una sola vez)."""
    return _store.pop(ref)


def peek(ref: str) -> Any:
    """Lee el dataset sin consumirlo (para que el flow padre arme sus estadísticas)."""
    return _store[ref]
