"""Clasificación de errores de extracción: define qué se reintenta y qué no."""


class SourceError(Exception):
    """Error base de una fuente externa."""


class TransientSourceError(SourceError):
    """Fallo pasajero (red, timeout, 429, 5xx, payload corrupto). Prefect lo reintenta."""


class RateLimitedError(TransientSourceError):
    """HTTP 429. Respeta Retry-After cuando la fuente lo envía."""


class MalformedPayloadError(TransientSourceError):
    """La respuesta llegó pero no se pudo interpretar (JSON truncado, estructura inesperada)."""


class PermanentSourceError(SourceError):
    """Fallo definitivo (400/401/403/404). Reintentar no lo arregla."""
