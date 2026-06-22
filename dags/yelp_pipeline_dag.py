"""
DAG principal del pipeline Yelp.
Orquesta las 6 etapas del proyecto sobre el día lógico {{ ds }}.
"""
from __future__ import annotations
import json
import logging
import time
from pathlib import Path
from datetime import datetime, timedelta
from typing import Iterator

from airflow import DAG
from airflow.operators.python import PythonOperator

from src.transform import clean
from src.loaders import mongo_loader, cassandra_loader, neo4j_loader
from src.kpis import compute_kpis
from src.common.config import STAGING_PATH

log = logging.getLogger(__name__)

default_args = {
    "owner": "jorge-qs",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

CHUNK_SIZE = 1_000   # registros por batch hacia cada BD


def _iter_jsonl(path: Path) -> Iterator[dict]:
    """Lee un JSONL línea a línea sin cargar todo en RAM."""
    if not path.exists():
        return
    with open(path, encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if line:
                yield json.loads(line)


def _chunked(it: Iterator[dict], size: int) -> Iterator[list[dict]]:
    """Agrupa un iterador en listas de tamaño `size`."""
    chunk: list[dict] = []
    for item in it:
        chunk.append(item)
        if len(chunk) >= size:
            yield chunk
            chunk = []
    if chunk:
        yield chunk


# ── Funciones de cada tarea ────────────────────────────────────────────────

def task_extract(**ctx):
    """Etapa 1: verifica que los archivos raw (o _sample / _demo) existen."""
    from src.common.config import RAW_DATA_PATH
    raw = Path(RAW_DATA_PATH)
    files = ["business.json", "review.json", "user.json", "checkin.json", "tip.json"]
    found, missing = [], []
    for f in files:
        base = f.replace(".json", "")
        for suffix in (f"{base}_demo.json", f"{base}_sample.json", f):
            if (raw / suffix).exists():
                found.append(suffix)
                break
        else:
            missing.append(f)
    if missing:
        raise FileNotFoundError(f"Archivos crudos faltantes (ni raw ni sample ni demo): {missing}")
    log.info("[extract] Archivos listos: %s", found)


def task_validate_clean(**ctx):
    """Etapa 2: limpiar y escribir staging para el día lógico."""
    ds = ctx["ds"]
    t0 = time.perf_counter()
    log.info("[validate_clean] Iniciando limpieza para ds=%s", ds)
    counts = clean.build_staging_for_date(ds)
    elapsed = time.perf_counter() - t0
    log.info(
        "[validate_clean] ds=%s listo en %.1fs | "
        "businesses=%d users=%d reviews=%d checkins=%d tips=%d",
        ds, elapsed,
        counts.get("businesses", 0), counts.get("users", 0),
        counts.get("reviews", 0), counts.get("checkins", 0), counts.get("tips", 0),
    )
    return counts


def task_load_mongo(**ctx):
    """Etapa 3: carga incremental en MongoDB con chunking anti-OOM."""
    ds = ctx["ds"]
    staging = Path(STAGING_PATH)
    t0 = time.perf_counter()
    totals = {"businesses": 0, "reviews": 0, "users": 0, "tips": 0}

    for chunk in _chunked(_iter_jsonl(staging / "businesses.jsonl"), CHUNK_SIZE):
        totals["businesses"] += mongo_loader.load_businesses(chunk)

    for chunk in _chunked(_iter_jsonl(staging / f"reviews/dt={ds}/part.jsonl"), CHUNK_SIZE):
        totals["reviews"] += mongo_loader.load_reviews(chunk)

    for chunk in _chunked(_iter_jsonl(staging / "users.jsonl"), CHUNK_SIZE):
        totals["users"] += mongo_loader.load_users(chunk)

    for chunk in _chunked(_iter_jsonl(staging / "tips.jsonl"), CHUNK_SIZE):
        totals["tips"] += mongo_loader.load_tips(chunk)

    log.info(
        "[load_mongo] ds=%s en %.1fs | businesses=%d reviews=%d users=%d tips=%d",
        ds, time.perf_counter() - t0,
        totals["businesses"], totals["reviews"], totals["users"], totals["tips"],
    )


def task_load_cassandra(**ctx):
    """Etapa 4: carga en Cassandra con categorías correctas."""
    ds = ctx["ds"]
    staging = Path(STAGING_PATH)
    t0 = time.perf_counter()

    # Mapa business_id → categorías: necesario para upsert_category_stats
    biz_cats: dict[str, list] = {}
    for b in _iter_jsonl(staging / "businesses.jsonl"):
        biz_cats[b["business_id"]] = b.get("categories", [])
    log.info("[load_cassandra] biz_cats cargado: %d negocios", len(biz_cats))

    reviews  = list(_iter_jsonl(staging / f"reviews/dt={ds}/part.jsonl"))
    checkins = list(_iter_jsonl(staging / f"checkins/dt={ds}/part.jsonl"))

    n_rv  = cassandra_loader.upsert_reviews_by_business(reviews)
    cassandra_loader.upsert_daily_counts(ds, reviews)
    n_cat = cassandra_loader.upsert_category_stats(ds, reviews, biz_cats)
    n_ck  = cassandra_loader.upsert_checkins(checkins)

    log.info(
        "[load_cassandra] ds=%s en %.1fs | reviews_by_biz=%d cat_stats=%d checkins=%d",
        ds, time.perf_counter() - t0, n_rv, n_cat, n_ck,
    )


def task_load_neo4j(**ctx):
    """Etapa 5: carga del grafo en Neo4j."""
    ds = ctx["ds"]
    staging = Path(STAGING_PATH)
    t0 = time.perf_counter()

    users_list = list(_iter_jsonl(staging / "users.jsonl"))
    biz_list   = list(_iter_jsonl(staging / "businesses.jsonl"))
    reviews    = list(_iter_jsonl(staging / f"reviews/dt={ds}/part.jsonl"))

    n_users   = neo4j_loader.upsert_users(users_list)
    n_biz     = neo4j_loader.upsert_businesses(biz_list)
    n_rev     = neo4j_loader.build_reviewed_edges(reviews)
    n_friends = neo4j_loader.build_friend_edges(users_list)
    n_cats    = neo4j_loader.build_category_edges(biz_list)

    log.info(
        "[load_neo4j] ds=%s en %.1fs | "
        "usuarios=%d negocios=%d reviews=%d amistades=%d categorias=%d",
        ds, time.perf_counter() - t0,
        n_users, n_biz, n_rev, n_friends, n_cats,
    )


def task_generate_kpis(**ctx):
    """Etapa 6: calcular y persistir KPIs."""
    ds = ctx["ds"]
    t0 = time.perf_counter()
    log.info("[generate_kpis] Calculando KPIs para ds=%s", ds)
    compute_kpis.run(ds)
    log.info("[generate_kpis] ds=%s listo en %.1fs", ds, time.perf_counter() - t0)


# ── Definición del DAG ─────────────────────────────────────────────────────

with DAG(
    dag_id="yelp_pipeline",
    description="Pipeline Yelp: extract → clean → MongoDB → Cassandra + Neo4j → KPIs",
    schedule="@daily",
    start_date=datetime(2019, 1, 26),
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    tags=["yelp", "bigdata", "utec"],
) as dag:

    extract        = PythonOperator(task_id="extract",                  python_callable=task_extract)
    validate       = PythonOperator(task_id="validate_clean",           python_callable=task_validate_clean)
    load_mongo     = PythonOperator(task_id="load_mongo",               python_callable=task_load_mongo)
    load_cassandra = PythonOperator(task_id="transform_load_cassandra", python_callable=task_load_cassandra)
    load_neo4j     = PythonOperator(task_id="build_load_neo4j",         python_callable=task_load_neo4j)
    gen_kpis       = PythonOperator(task_id="generate_kpis",            python_callable=task_generate_kpis)

    extract >> validate >> load_mongo >> [load_cassandra, load_neo4j] >> gen_kpis
