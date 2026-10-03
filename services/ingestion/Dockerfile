# Una sola imagen para scheduler, workers de Dask y el proceso de Prefect:
# garantiza versiones idénticas de dask/distributed/prefect en todo el clúster
# y que los workers puedan importar el código de las tasks que reciben.
FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1 \
    PYTHONPATH=/app

WORKDIR /app
COPY requirements.txt .
RUN pip install -r requirements.txt

COPY wandersync_ingest ./wandersync_ingest

RUN useradd --create-home --uid 10001 wander \
    && mkdir -p /home/wander/.prefect \
    && chown -R wander /app /home/wander
USER wander

CMD ["python", "-m", "wandersync_ingest.serve"]
