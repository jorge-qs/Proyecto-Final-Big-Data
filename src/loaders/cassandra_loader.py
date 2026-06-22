"""
Carga de datos en Cassandra (series temporales y agregados).
Dueño: Rol 3 — Cassandra.
"""
from __future__ import annotations
import logging
from collections import defaultdict
from datetime import date, datetime
from cassandra.cluster import Cluster
from cassandra.concurrent import execute_concurrent_with_args
from src.common.config import CASSANDRA_HOSTS, CASSANDRA_PORT, CASSANDRA_KEYSPACE

logger = logging.getLogger(__name__)


def _to_date(v) -> date:
    if isinstance(v, date):
        return v
    return date.fromisoformat(str(v)[:10])


def _to_datetime(v) -> datetime:
    if isinstance(v, datetime):
        return v
    return datetime.fromisoformat(str(v).replace("Z", ""))


_session = None


def _get_session():
    global _session
    if _session is None:
        cluster = Cluster(CASSANDRA_HOSTS, port=CASSANDRA_PORT)
        _session = cluster.connect(CASSANDRA_KEYSPACE)
    return _session


def upsert_reviews_by_business(records: list[dict]) -> int:
    if not records:
        return 0
    session = _get_session()
    stmt = session.prepare("""
        INSERT INTO reviews_by_business_date
        (business_id, review_date, review_id, stars, user_id)
        VALUES (?, ?, ?, ?, ?)
    """)
    params = [
        (r["business_id"], _to_date(r["date"]), r["review_id"], float(r["stars"]), r["user_id"])
        for r in records
    ]
    execute_concurrent_with_args(session, stmt, params, concurrency=50)
    return len(params)


def upsert_daily_counts(ds: str, records: list[dict]) -> int:
    if not records:
        return 0
    session = _get_session()
    year_month = ds[:7]
    total = len(records)
    stmt = session.prepare("""
        INSERT INTO daily_review_counts (year_month, review_date, total)
        VALUES (?, ?, ?)
    """)
    session.execute(stmt, (year_month, _to_date(ds), total))
    return total


def upsert_category_stats(
    ds: str,
    reviews: list[dict],
    biz_cats: dict[str, list] | None = None,
) -> int:
    """
    Calcula y persiste estadísticas por categoría para el día ds.

    biz_cats: {business_id: [category, ...]} — mapeo necesario para asociar
    cada review a sus categorías. Si no se pasa, intenta usar el campo
    'categories' del propio registro (sólo presente tras join previo).
    """
    if not reviews:
        return 0
    biz_cats = biz_cats or {}
    session = _get_session()

    stats: dict[str, list[tuple[float, float]]] = defaultdict(list)
    for r in reviews:
        cats = biz_cats.get(r.get("business_id", ""), r.get("categories", []))
        for cat in cats:
            if cat:
                stats[cat].append((float(r.get("stars", 0)), float(r.get("sentiment") or 0)))

    if not stats:
        logger.warning(
            "upsert_category_stats[%s]: 0 categorías — verifica que biz_cats no esté vacío", ds
        )
        return 0

    stmt = session.prepare("""
        INSERT INTO category_daily_stats
        (category, stat_date, review_count, avg_stars, avg_sentiment)
        VALUES (?, ?, ?, ?, ?)
    """)
    params = []
    for cat, vals in stats.items():
        avg_stars = sum(v[0] for v in vals) / len(vals)
        avg_sent  = sum(v[1] for v in vals) / len(vals)
        params.append((cat, _to_date(ds), len(vals), avg_stars, avg_sent))

    execute_concurrent_with_args(session, stmt, params, concurrency=50)
    logger.info("upsert_category_stats[%s]: %d categorías escritas", ds, len(params))
    return len(params)


def upsert_checkins(records: list[dict]) -> int:
    if not records:
        return 0
    session = _get_session()
    stmt = session.prepare("""
        INSERT INTO checkins_by_business (business_id, checkin_ts)
        VALUES (?, ?)
    """)
    params = [(r["business_id"], _to_datetime(r["checkin_ts"])) for r in records]
    execute_concurrent_with_args(session, stmt, params, concurrency=50)
    return len(params)
