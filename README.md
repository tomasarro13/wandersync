# WanderSync Travel Solutions

Plataforma de empaquetamiento turístico dinámico (vuelo + hotel + auto).
Parcial práctico · Patrones Arquitectónicos Avanzados · Universidad de La Sabana.

## Estado actual

| Módulo | Estado |
|---|---|
| Ingesta distribuida (Dask + Prefect) | ✅ funcionando |
| Proveedores simulados con modo caos | ✅ funcionando |
| Esquema de catálogo en Supabase | ✅ `supabase/migrations/001_catalog.sql` |
| Microservicios Node (vuelos, hoteles, autos, órdenes) | ⏳ siguiente fase |
| API Gateway GraphQL (Apollo) + seguridad | ⏳ |
| SAGA de reservas | ⏳ |
| Frontend | ⏳ |

## Puesta en marcha

1. **Variables de entorno**: `cp .env.example .env` y completa las claves de Supabase y el token de prueba de Duffel.
   Sin `.env` todo funciona igual en modo *dry-run* (no escribe en Supabase y omite Duffel).
2. **Esquema**: en Supabase → *SQL Editor*, ejecuta `supabase/migrations/001_catalog.sql`.
3. **Levantar**: `docker compose up -d --build`

| Interfaz | URL |
|---|---|
| Prefect (flows, tasks, logs, artifacts, events) | http://localhost:4200 |
| Dashboard de Dask (task stream, progreso, workers) | http://localhost:8787/status |
| Panel de caos de los proveedores simulados | http://localhost:4010/__chaos |

## Ingesta: fuentes de datos

| Dominio | Fuente | Tipo |
|---|---|---|
| Hoteles | TiquetesBaratos `/ai/` (endpoints públicos documentados en su `llms.txt`) | **Real** |
| Vuelos | Duffel API, modo test (aerolínea "Duffel Airways") | **Real (sandbox)** |
| Vuelos | Feed de aerolíneas colombianas (Avianca, LATAM, Wingo, SATENA, Clic) | Simulado |
| Autos | Proveedor de alquiler (Localiza, Hertz, Avis, Budget, Europcar) | Simulado |

TiquetesBaratos entrega el catálogo real pero sin precios: las tarifas por noche las calcula
un motor de precios distribuido (Dask Bag). El scraper es de bajo volumen: User-Agent
identificable, pausa entre peticiones y máximo 2 peticiones simultáneas (límite de concurrencia de Prefect).

## Cómo correr una ingesta

- **Desde la UI de Prefect**: *Deployments* → `wandersync-ingesta-maestra/demo-manual` → **Run**.
- **Desde la terminal**: `docker compose exec ingestion prefect deployment run 'wandersync-ingesta-maestra/demo-manual'`
- También hay deployments por dominio: `ingesta-hoteles/manual`, `ingesta-vuelos/manual`, `ingesta-autos/manual`.
- La ingesta completa se programa cada hora (`INGESTION_CRON`); se desactiva con `INGESTION_SCHEDULE_ENABLED=false`.

## Qué se observa

**En Prefect**
- Jerarquía `wandersync-ingesta-maestra → ingesta-<dominio> → <dominio>-1-extraccion / -2-transformacion / -3-carga`.
- Cada task lleva el nombre del método que ejecuta y su recurso: `TiquetesBaratosHotelScraper.fetch_listing_page[CTG·p2]`.
- Dentro de cada task, el decorador `@traced` registra **cada invocación de función o método al entrar**
  (`▶ ENTRA`), al salir (`◀ SALE`) o al fallar (`✖ ERROR`), con argumentos, worker de Dask, número de intento y duración.
  Además emite eventos `wandersync.fn.started/completed/failed` (pestaña *Events*).
- Reintentos: 3 con backoff exponencial (2 s, 4 s, 8 s) ± 50 % de jitter, **solo** para errores transitorios
  (timeout, 429, 5xx, JSON corrupto). Los 4xx fallan de inmediato.
- Artifacts: progreso en vivo de cada etapa, tabla con el resultado de cada petición (intentos y worker),
  tabla de particiones de Dask, precios por ruta y un resumen por dominio.

**En Dask**
- Clúster de 1 scheduler + 3 workers (`worker-1..3`, 2 hilos c/u).
- Cada tarea tiene una key legible `<Clase.metodo>-<id>-<parámetros>`: la vista *Progress* muestra una barra por método.
- Cómputo distribuido explícito: `HotelRateEngine.price_partition` (Dask Bag, 24 particiones) y
  `RouteStatsCalculator` (Dask DataFrame `groupby`). Cada partición reporta qué worker la procesó.

## Modo caos (fallos simulados)

El servicio `mock-sources` falla al azar (25 % por defecto) con: **503**, **429**, **timeout** (responde a los 20 s,
el cliente corta a los 8 s) y **JSON truncado**. Además hay recursos "envenenados" que siempre fallan para
mostrar reintentos agotados sin tumbar el flow: ruta `BOG-LET` y ubicación `LET`.

```bash
# ver configuración y contadores
curl localhost:4010/__chaos
# subir la tasa de fallos al 60 % en caliente
curl -X POST localhost:4010/__chaos -H 'content-type: application/json' -d '{"failureRate":0.6}'
# apagar el caos
curl -X POST localhost:4010/__chaos -H 'content-type: application/json' -d '{"enabled":false}'
```

## Estructura

```
docker-compose.yml
supabase/migrations/        esquema SQL (catálogo + RLS + pg_graphql)
services/
  mock-sources/             Node · proveedores simulados + modo caos
  ingestion/                Python · Dask + Prefect
    wandersync_ingest/
      tracing.py            @traced: trazado por invocación
      runner.py             DaskTaskRunner con keys legibles + política de reintentos
      sources/              TiquetesBaratos, Duffel, mocks (clasificación de errores)
      transform/            limpieza, motor de tarifas (Dask Bag), estadísticas (Dask DataFrame)
      persistence/          Supabase vía PostgREST (upsert por lotes)
      flows/                flows y subflows de Prefect
      serve.py              registra deployments y límites de concurrencia
```
