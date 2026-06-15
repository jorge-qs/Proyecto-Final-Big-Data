"""
Calcula los 6 KPIs del proyecto y los escribe en kpi_results (Cassandra).
Dueño: Rol 5 — Airflow + Dashboard.
"""
import logging
from datetime import date
from src.common.config import CASSANDRA_KEYSPACE, CASSANDRA_HOSTS, CASSANDRA_PORT, MONGO_URI, NEO4J_URI, NEO4J_USER, NEO4J_PASSWORD
from cassandra.cluster import Cluster
from pymongo import MongoClient
from neo4j import GraphDatabase

logger = logging.getLogger(__name__)


def _cassandra():
    cluster = Cluster(CASSANDRA_HOSTS, port=CASSANDRA_PORT)
    return cluster.connect(CASSANDRA_KEYSPACE)

def _mongo():
    return MongoClient(MONGO_URI)["yelp"]

def _neo4j():
    return GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASSWORD))


def _write_kpi(session, kpi_name: str, kpi_date: str, value: float, detail: str = "") -> None:
    session.execute("""
        INSERT INTO kpi_results (kpi_name, kpi_date, value, detail)
        VALUES (%s, %s, %s, %s)
    """, (kpi_name, kpi_date, value, detail))


def run(ds: str) -> None:
    """Calcula todos los KPIs para el día ds y los persiste en Cassandra."""
    session = _cassandra()
    kpi_date = date.fromisoformat(ds)
    year_month = ds[:7]

    # KPI 1 — Reseñas procesadas por día
    row = session.execute("SELECT total FROM daily_review_counts WHERE year_month=%s AND review_date=%s",
                          (year_month, kpi_date)).one()
    kpi1 = float(row.total) if row else 0.0
    _write_kpi(session, "reviews_per_day", ds, kpi1, f"Reseñas procesadas el {ds}")

    # KPI 2 — Crecimiento diario (%)
    from datetime import timedelta
    prev_ds = (kpi_date - timedelta(days=1)).isoformat()
    prev_month = prev_ds[:7]
    row_prev = session.execute("SELECT total FROM daily_review_counts WHERE year_month=%s AND review_date=%s",
                               (prev_month, date.fromisoformat(prev_ds))).one()
    prev = float(row_prev.total) if row_prev else 0.0
    kpi2 = ((kpi1 - prev) / prev * 100) if prev > 0 else 0.0
    _write_kpi(session, "daily_growth_pct", ds, kpi2, f"Crecimiento vs {prev_ds}")

    # KPI 3 — Rating promedio por categoría (top 1 en Cassandra)
    rows3 = session.execute("SELECT category, avg_stars FROM category_daily_stats WHERE stat_date=%s ALLOW FILTERING",
                            (kpi_date,))
    best = max(rows3, key=lambda r: r.avg_stars, default=None)
    kpi3 = float(best.avg_stars) if best else 0.0
    detail3 = best.category if best else ""
    _write_kpi(session, "top_category_avg_stars", ds, kpi3, detail3)

    # KPI 4 — Usuario más influyente (grado en Neo4j)
    driver = _neo4j()
    with driver.session() as ns:
        result4 = ns.run("MATCH (u:User)-[:FRIEND]->(f:User) RETURN u.user_id AS uid, count(f) AS deg ORDER BY deg DESC LIMIT 1")
        rec4 = result4.single()
        kpi4 = float(rec4["deg"]) if rec4 else 0.0
        detail4 = rec4["uid"] if rec4 else ""
    _write_kpi(session, "top_influencer_degree", ds, kpi4, detail4)

    # KPI 5 — Negocio más reseñado por la red del influencer
    with driver.session() as ns:
        result5 = ns.run("""
            MATCH (:User {user_id:$uid})-[:FRIEND]->(:User)-[:REVIEWED]->(b:Business)
            RETURN b.name AS bname, count(*) AS cnt ORDER BY cnt DESC LIMIT 1
        """, uid=detail4)
        rec5 = result5.single()
        kpi5 = float(rec5["cnt"]) if rec5 else 0.0
        detail5 = rec5["bname"] if rec5 else ""
    _write_kpi(session, "top_business_network", ds, kpi5, detail5)

    # KPI 6 — Sentimiento promedio del día
    rows6 = session.execute("SELECT avg_sentiment FROM category_daily_stats WHERE stat_date=%s ALLOW FILTERING",
                            (kpi_date,))
    sents = [r.avg_sentiment for r in rows6 if r.avg_sentiment is not None]
    kpi6 = sum(sents) / len(sents) if sents else 0.0
    _write_kpi(session, "avg_sentiment", ds, kpi6, f"Sentimiento medio del {ds}")

    driver.close()
    logger.info("KPIs calculados para %s", ds)
