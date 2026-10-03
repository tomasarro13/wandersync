-- =====================================================================
-- WanderSync · 001 · Catálogo turístico (destino de la ingesta Dask/Prefect)
-- Ejecutar en Supabase → SQL Editor.  Idempotente: se puede correr varias veces.
-- =====================================================================

-- pg_graphql expone el esquema en /graphql/v1 (en proyectos nuevos viene desactivado)
create extension if not exists pg_graphql;

-- pg_graphql: nombres camelCase en el endpoint /graphql/v1
comment on schema public is e'@graphql({"inflect_names": true})';

-- ---------------------------------------------------------------------
-- Registro de ejecuciones de ingesta (una fila por flow run de Prefect)
-- ---------------------------------------------------------------------
create table if not exists public.ingestion_runs (
  id            uuid primary key default gen_random_uuid(),
  flow_name     text        not null,
  flow_run_id   text,
  status        text        not null default 'running',   -- running | completed | completed_with_errors | failed
  started_at    timestamptz not null default now(),
  finished_at   timestamptz,
  stats         jsonb       not null default '{}'::jsonb
);

-- ---------------------------------------------------------------------
-- Vuelos (Duffel test + feed de aerolíneas colombianas mock)
-- ---------------------------------------------------------------------
create table if not exists public.flights (
  id                 text primary key,              -- <source>:<clave estable de la oferta>
  source             text        not null,          -- duffel | mock-airlines
  origin             char(3)     not null,
  destination        char(3)     not null,
  departure_at       timestamptz not null,
  arrival_at         timestamptz not null,
  duration_minutes   int,
  airline_code       text,
  airline_name       text,
  flight_number      text,
  stops              int         not null default 0,
  cabin_class        text        not null default 'economy',
  price              numeric(14,2) not null,        -- normalizado a COP
  currency           char(3)     not null default 'COP',
  original_price     numeric(14,2),
  original_currency  char(3),
  seats_available    int,
  external_offer_id  text,
  scraped_at         timestamptz not null default now(),
  ingestion_run_id   uuid references public.ingestion_runs(id) on delete set null
);
create index if not exists flights_route_idx on public.flights (origin, destination, departure_at);

-- ---------------------------------------------------------------------
-- Hoteles (TiquetesBaratos /ai/ — catálogo real)
-- ---------------------------------------------------------------------
create table if not exists public.hotels (
  id                text primary key,               -- tb:<id externo>
  source            text        not null,
  external_id       text        not null,
  name              text        not null,
  city              text,
  destination_code  char(3),                        -- IATA del destino (enlaza con vuelos)
  stars             numeric(2,1),
  rating            numeric(3,1),
  reviews           int,
  lat               double precision,
  lng               double precision,
  thumbnail         text,
  amenities         text[]      not null default '{}',
  highlights        text[]      not null default '{}',
  room_types        text[]      not null default '{}',
  is_all_inclusive  boolean     not null default false,
  booking_url       text,
  scraped_at        timestamptz not null default now(),
  ingestion_run_id  uuid references public.ingestion_runs(id) on delete set null
);
create index if not exists hotels_dest_idx on public.hotels (destination_code);

-- Tarifas calculadas por el motor de precios distribuido (Dask Bag)
create table if not exists public.hotel_rates (
  id               text primary key,                -- <hotel_id>:<room>:<fecha>
  hotel_id         text        not null references public.hotels(id) on delete cascade,
  stay_date        date        not null,
  room_type        text        not null,
  price_per_night  numeric(14,2) not null,
  currency         char(3)     not null default 'COP',
  rooms_available  int         not null,
  computed_at      timestamptz not null default now()
);
create index if not exists hotel_rates_lookup_idx on public.hotel_rates (hotel_id, stay_date);

-- ---------------------------------------------------------------------
-- Autos (proveedor de alquiler mock)
-- ---------------------------------------------------------------------
create table if not exists public.cars (
  id                text primary key,
  source            text        not null,
  city              text        not null,
  destination_code  char(3)     not null,
  pickup_date       date        not null,
  company           text        not null,
  model             text        not null,
  category          text        not null,
  transmission      text        not null,
  seats             int         not null,
  bags              int         not null,
  air_conditioning  boolean     not null default true,
  price_per_day     numeric(14,2) not null,
  currency          char(3)     not null default 'COP',
  units_available   int         not null,
  scraped_at        timestamptz not null default now(),
  ingestion_run_id  uuid references public.ingestion_runs(id) on delete set null
);
create index if not exists cars_dest_idx on public.cars (destination_code, pickup_date);

-- Estadísticas por ruta (Dask DataFrame groupby)
create table if not exists public.route_price_stats (
  route               text primary key,             -- BOG-CTG
  origin              char(3) not null,
  destination         char(3) not null,
  min_price           numeric(14,2) not null,
  avg_price           numeric(14,2) not null,
  max_price           numeric(14,2) not null,
  offers              int     not null,
  cheapest_flight_id  text,
  computed_at         timestamptz not null default now()
);

-- ---------------------------------------------------------------------
-- RLS: el catálogo es de lectura pública; solo la service key escribe.
-- ---------------------------------------------------------------------
do $$
declare t text;
begin
  foreach t in array array['ingestion_runs','flights','hotels','hotel_rates','cars','route_price_stats'] loop
    execute format('alter table public.%I enable row level security', t);
    execute format('drop policy if exists "lectura publica" on public.%I', t);
    execute format('create policy "lectura publica" on public.%I for select to anon, authenticated using (true)', t);
  end loop;
end $$;
