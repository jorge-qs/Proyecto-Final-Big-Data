-- Ejecutar con: cqlsh -f scripts/cassandra_schema.cql

CREATE KEYSPACE IF NOT EXISTS yelp_analytics
  WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};

USE yelp_analytics;

-- Reseñas por negocio y fecha (consulta: "dame las reseñas de este negocio ordenadas por fecha")
CREATE TABLE IF NOT EXISTS reviews_by_business_date (
  business_id  text,
  review_date  date,
  review_id    text,
  stars        float,
  user_id      text,
  PRIMARY KEY ((business_id), review_date, review_id)
) WITH CLUSTERING ORDER BY (review_date DESC);

-- Conteo diario global — KPI 1 y 2
-- Particionado por año-mes para que la partición no crezca infinito
CREATE TABLE IF NOT EXISTS daily_review_counts (
  year_month   text,
  review_date  date,
  total        int,
  PRIMARY KEY ((year_month), review_date)
) WITH CLUSTERING ORDER BY (review_date ASC);

-- Stats por categoría y día — KPI 3 y 6
CREATE TABLE IF NOT EXISTS category_daily_stats (
  category     text,
  stat_date    date,
  review_count int,
  avg_stars    float,
  avg_sentiment float,
  PRIMARY KEY ((category), stat_date)
) WITH CLUSTERING ORDER BY (stat_date ASC);

-- Check-ins por negocio
CREATE TABLE IF NOT EXISTS checkins_by_business (
  business_id  text,
  checkin_ts   timestamp,
  PRIMARY KEY ((business_id), checkin_ts)
) WITH CLUSTERING ORDER BY (checkin_ts DESC);

-- Resultados de KPIs para el dashboard
CREATE TABLE IF NOT EXISTS kpi_results (
  kpi_name     text,
  kpi_date     date,
  value        double,
  detail       text,
  PRIMARY KEY ((kpi_name), kpi_date)
) WITH CLUSTERING ORDER BY (kpi_date DESC);
