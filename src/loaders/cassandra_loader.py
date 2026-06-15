"""
Carga de datos en Cassandra (series temporales y agregados).
Dueño: Rol 3 — Cassandra.
"""
from __future__ import annotations
from datetime import date, datetime
from cassandra.cluster import Cluster
from cassandra.concurrent import execute_concurrent_with_args
from src.common.config import CASSANDRA_HOSTS, CASSANDRA_PORT, CASSANDRA_KEYSPACE

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
    params = [(r["business_id"], _to_date(r["date"]), r["review_id"], float(r["stars"]), r["user_id"]) for r in records]
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


def upsert_category_stats(ds: str, records: list[dict]) -> int:
    if not records:
        return 0
    from collections import defaultdict
    session = _get_session()
    stats: dict[str, list] = defaultdict(list)
    for r in records:
        for cat in r.get("categories", []):
            stats[cat].append((r.get("stars", 0), r.get("sentiment", 0)))

    stmt = session.prepare("""
        INSERT INTO category_daily_stats
        (category, stat_date, review_count, avg_stars, avg_sentiment)
        VALUES (?, ?, ?, ?, ?)
    """)
    params = []
    for cat, vals in stats.items():
        avg_stars = sum(v[0] for v in vals) / len(vals)
        avg_sent = sum(v[1] for v in vals) / len(vals)
        params.append((cat, _to_date(ds), len(vals), avg_stars, avg_sent))
    execute_concurrent_with_args(session, stmt, params, concurrency=50)
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
