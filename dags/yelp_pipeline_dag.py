"""
DAG principal del pipeline Yelp.
Orquesta las 6 etapas del proyecto sobre el día lógico {{ ds }}.
"""
from __future__ import annotations
import json
from pathlib import Path
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

from src.transform import clean
from src.loaders import mongo_loader, cassandra_loader, neo4j_loader
from src.kpis import compute_kpis
from src.common.config import STAGING_PATH

default_args = {
    "owner": "jorge-qs",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}


def _read_jsonl(path: str) -> list[dict]:
    p = Path(path)
    if not p.exists():
        return []
    return [json.loads(line) for line in p.read_text().splitlines() if line.strip()]


# ── Funciones de cada tarea ────────────────────────────────────────────────

def task_extract(**ctx):
    """Etapa 1: verificar que los archivos raw existen."""
    from src.common.config import RAW_DATA_PATH
    raw = Path(RAW_DATA_PATH)
    missing = [f for f in ["business.json", "review.json", "user.json", "checkin.json", "tip.json"]
               if not (raw / f).exists()]
    if missing:
        raise FileNotFoundError(f"Archivos crudos faltantes: {missing}")
    print("Raw data OK:", list(raw.glob("*.json")))


def task_validate_clean(**ctx):
    """Etapa 2: limpiar y escribir staging para el día lógico."""
    ds = ctx["ds"]
    counts = clean.build_staging_for_date(ds)
    print("Staging generado:", counts)
    return counts


def task_load_mongo(**ctx):
    """Etapa 3: cargar en MongoDB."""
    ds = ctx["ds"]
    staging = Path(STAGING_PATH)
    businesses = _read_jsonl(staging / "businesses.jsonl")
    reviews = _read_jsonl(staging / f"reviews/dt={ds}/part.jsonl")
    users = _read_jsonl(staging / "users.jsonl")
    tips = _read_jsonl(staging / "tips.jsonl")
    n = (mongo_loader.load_businesses(businesses) +
         mongo_loader.load_reviews(reviews) +
         mongo_loader.load_users(users) +
         mongo_loader.load_tips(tips))
    print(f"MongoDB: {n} operaciones")


def task_load_cassandra(**ctx):
    """Etapa 4: cargar en Cassandra."""
    ds = ctx["ds"]
    staging = Path(STAGING_PATH)
    reviews = _read_jsonl(staging / f"reviews/dt={ds}/part.jsonl")
    checkins = _read_jsonl(staging / f"checkins/dt={ds}/part.jsonl")
    cassandra_loader.upsert_reviews_by_business(reviews)
    cassandra_loader.upsert_daily_counts(ds, reviews)
    cassandra_loader.upsert_category_stats(ds, reviews)
    cassandra_loader.upsert_checkins(checkins)
    print(f"Cassandra: {len(reviews)} reviews, {len(checkins)} checkins")


def task_load_neo4j(**ctx):
    """Etapa 5: cargar grafo en Neo4j."""
    staging = Path(STAGING_PATH)
    users = _read_jsonl(staging / "users.jsonl")
    businesses = _read_jsonl(staging / "businesses.jsonl")
    ds = ctx["ds"]
    reviews = _read_jsonl(staging / f"reviews/dt={ds}/part.jsonl")
    neo4j_loader.upsert_users(users)
    neo4j_loader.upsert_businesses(businesses)
    neo4j_loader.build_friend_edges(users)
    neo4j_loader.build_reviewed_edges(reviews)
    neo4j_loader.build_category_edges(businesses)
    print(f"Neo4j: {len(users)} usuarios, {len(businesses)} negocios, {len(reviews)} reviews")


def task_generate_kpis(**ctx):
    """Etapa 6: calcular y persistir KPIs."""
    compute_kpis.run(ctx["ds"])
    print("KPIs generados para", ctx["ds"])


# ── Definición del DAG ─────────────────────────────────────────────────────

with DAG(
    dag_id="yelp_pipeline",
    description="Pipeline completo Yelp: extract → clean → MongoDB → Cassandra + Neo4j → KPIs",
    schedule="@daily",
    start_date=datetime(2019, 1, 26),   # solo 1 día atrás para pruebas
    catchup=False,                       # sin backfill en desarrollo
    max_active_runs=1,
    default_args=default_args,
    tags=["yelp", "bigdata", "utec"],
) as dag:

    extract = PythonOperator(task_id="extract", python_callable=task_extract)
    validate = PythonOperator(task_id="validate_clean", python_callable=task_validate_clean)
    load_mongo = PythonOperator(task_id="load_mongo", python_callable=task_load_mongo)
    load_cassandra = PythonOperator(task_id="transform_load_cassandra", python_callable=task_load_cassandra)
    load_neo4j = PythonOperator(task_id="build_load_neo4j", python_callable=task_load_neo4j)
    gen_kpis = PythonOperator(task_id="generate_kpis", python_callable=task_generate_kpis)

    extract >> validate >> load_mongo >> [load_cassandra, load_neo4j] >> gen_kpis
