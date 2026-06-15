# Rol 3 — Cassandra (series temporales y agregados)

> Tu mantra: **diseña las tablas a partir de las consultas, no de las entidades**
> (query-first). En Cassandra se denormaliza y se duplica dato a propósito.

---

## Objetivo
Modelar tablas optimizadas para consultas masivas por fecha, cargar los agregados y
exponer funciones reutilizables para Airflow. Cubres la **etapa 4 del DAG**.

## Tu alcance
- `src/loaders/cassandra_loader.py`
- `scripts/cassandra_schema.cql`
- La tabla `kpi_results` que el dashboard lee (coordínala con el de Airflow).

## Consumes
`data/staging/` (reseñas y check-ins particionados por fecha).

---

## Modelo de datos (query-first — justifícalo)

Keyspace `yelp_analytics`. Diseña **una tabla por patrón de consulta**:

```sql
-- Reseñas en el tiempo para un negocio
CREATE TABLE reviews_by_business_date (
  business_id text, review_date date, review_id text,
  stars float, user_id text,
  PRIMARY KEY ((business_id), review_date, review_id)
) WITH CLUSTERING ORDER BY (review_date DESC);

-- Conteo diario global (KPI 1 y 2) — partición por mes para no crecer infinito
CREATE TABLE daily_review_counts (
  year_month text, review_date date, total int,
  PRIMARY KEY ((year_month), review_date)
) WITH CLUSTERING ORDER BY (review_date ASC);

-- Estadísticos por categoría y día (KPI 3)
CREATE TABLE category_daily_stats (
  category text, stat_date date,
  review_count int, avg_stars float, avg_sentiment float,
  PRIMARY KEY ((category), stat_date)
) WITH CLUSTERING ORDER BY (stat_date ASC);

-- Check-ins por negocio (serie temporal)
CREATE TABLE checkins_by_business (
  business_id text, checkin_ts timestamp,
  PRIMARY KEY ((business_id), checkin_ts)
) WITH CLUSTERING ORDER BY (checkin_ts DESC);

-- Resultados de KPIs que lee el dashboard (la llena la etapa generate_kpis)
CREATE TABLE kpi_results (
  kpi_name text, kpi_date date, value double, detail text,
  PRIMARY KEY ((kpi_name), kpi_date)
) WITH CLUSTERING ORDER BY (kpi_date DESC);
```

**Reglas de oro:** clave de partición = lo que filtras; clustering = cómo ordenas;
nunca `ALLOW FILTERING` en producción; partición acotada (por eso `year_month`).

---

## Funciones para Airflow
```python
# src/loaders/cassandra_loader.py
def upsert_reviews_by_business(records: list[dict]) -> int: ...
def upsert_daily_counts(ds: str, records: list[dict]) -> int: ...
def upsert_category_stats(ds: str, records: list[dict]) -> int: ...
def upsert_checkins(records: list[dict]) -> int: ...
```
Las escrituras en Cassandra son upsert por naturaleza → idempotencia gratis. Usa
`execute_concurrent` o `BatchStatement` por partición para velocidad.

---

## CRUD (demostrar en vivo)
- **Inserción:** `INSERT` en `reviews_by_business_date`.
- **Consulta:** `SELECT` por partición (reseñas de un `business_id` ordenadas por fecha).
- **Actualización:** `UPDATE` de un contador / stat.
- **Eliminación:** `DELETE` de una fila por clave completa.

## Qué documentar
- Cada tabla + la consulta que justifica su diseño.
- Por qué denormalizaste y elegiste esas claves de partición/clustering.
- Capturas de los 4 CRUD y de un `SELECT` de serie temporal.

## Definición de "terminado"
- [ ] `cassandra_schema.cql` aplicado, tablas creadas.
- [ ] `cassandra_loader.py` probado e idempotente.
- [ ] `kpi_results` lista para que el dashboard la lea.
- [ ] CRUD demostrable.

## Prompt sugerido para tu agente
> "Soy el responsable de Cassandra en un proyecto Big Data sobre Yelp. Consumo
> `.jsonl` limpios de `data/staging/` (reseñas y check-ins por fecha). Ayúdame a: (1)
> escribir `scripts/cassandra_schema.cql` con tablas query-first para conteos
> diarios, stats por categoría, reseñas por negocio, check-ins y una tabla
> kpi_results; (2) `src/loaders/cassandra_loader.py` con `cassandra-driver`,
> escrituras concurrentes idempotentes y funciones de upsert por tabla; (3) un script
> de demostración CRUD. Explica las claves de partición y clustering elegidas."
