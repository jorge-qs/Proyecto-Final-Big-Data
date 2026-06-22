"""
Calcula los 6 KPIs del proyecto y los escribe en kpi_results (Cassandra).
Dueño: Rol 5 — Airflow + Dashboard.
"""
import logging
from datetime import date, timedelta
from src.common.config import (
    CASSANDRA_KEYSPACE, CASSANDRA_HOSTS, CASSANDRA_PORT,
    NEO4J_URI, NEO4J_USER, NEO4J_PASSWORD,
)
from cassandra.cluster import Cluster
from neo4j import GraphDatabase

logger = logging.getLogger(__name__)


def _cassandra():
    cluster = Cluster(CASSANDRA_HOSTS, port=CASSANDRA_PORT)
    return cluster.connect(CASSANDRA_KEYSPACE)


def _neo4j():
    return GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASSWORD))


def _write_kpi(session, kpi_name: str, kpi_date: str, value: float, detail: str = "") -> None:
    session.execute(
        "INSERT INTO kpi_results (kpi_name, kpi_date, value, detail) VALUES (%s, %s, %s, %s)",
        (kpi_name, kpi_date, value, detail),
    )
    logger.info("KPI %-28s [%s] = %10.4f  | %s", kpi_name, kpi_date, value, detail)


def run(ds: str) -> None:
    """Calcula todos los KPIs para el día ds y los persiste en Cassandra."""
    session = _cassandra()
    driver = _neo4j()
    kpi_date = date.fromisoformat(ds)
    year_month = ds[:7]

    try:
        # ── KPI 1 — Reseñas procesadas por día ──────────────────────────────
        row = session.execute(
            "SELECT total FROM daily_review_counts WHERE year_month=%s AND review_date=%s",
            (year_month, kpi_date),
        ).one()
        kpi1 = float(row.total) if row else 0.0
        _write_kpi(session, "reviews_per_day", ds, kpi1, f"Reseñas procesadas el {ds}")

        # ── KPI 2 — Crecimiento diario (%) ──────────────────────────────────
        prev_ds = (kpi_date - timedelta(days=1)).isoformat()
        row_prev = session.execute(
            "SELECT total FROM daily_review_counts WHERE year_month=%s AND review_date=%s",
            (prev_ds[:7], date.fromisoformat(prev_ds)),
        ).one()
        prev = float(row_prev.total) if row_prev else 0.0
        kpi2 = ((kpi1 - prev) / prev * 100) if prev > 0 else 0.0
        _write_kpi(session, "daily_growth_pct", ds, kpi2, f"Crecimiento vs {prev_ds}")

        # ── KPI 3 — Rating promedio de la mejor categoría ───────────────────
        rows3 = list(session.execute(
            "SELECT category, avg_stars FROM category_daily_stats "
            "WHERE stat_date=%s ALLOW FILTERING",
            (kpi_date,),
        ))
        best3 = max(rows3, key=lambda r: r.avg_stars, default=None)
        kpi3 = float(best3.avg_stars) if best3 else 0.0
        detail3 = best3.category if best3 else "sin datos"
        _write_kpi(session, "top_category_avg_stars", ds, kpi3, detail3)

        # ── KPI 4 — Usuario más influyente (grado en Neo4j) ─────────────────
        detail4 = ""
        kpi4 = 0.0
        with driver.session() as ns:
            rec4 = ns.run(
                "MATCH (u:User)-[:FRIEND]->(f:User) "
                "RETURN u.user_id AS uid, count(f) AS deg "
                "ORDER BY deg DESC LIMIT 1"
            ).single()
            if rec4:
                kpi4 = float(rec4["deg"])
                detail4 = rec4["uid"]
        _write_kpi(session, "top_influencer_degree", ds, kpi4, detail4)

        # ── KPI 5 — Negocio más reseñado por la red del influencer ──────────
        kpi5 = 0.0
        detail5 = "sin datos"
        if detail4:
            with driver.session() as ns:
                rec5 = ns.run(
                    "MATCH (:User {user_id:$uid})-[:FRIEND]->(:User)-[:REVIEWED]->(b:Business) "
                    "RETURN b.name AS bname, count(*) AS cnt ORDER BY cnt DESC LIMIT 1",
                    uid=detail4,
                ).single()
                if rec5:
                    kpi5 = float(rec5["cnt"])
                    detail5 = rec5["bname"] or ""
        _write_kpi(session, "top_business_network", ds, kpi5, detail5)

        # ── KPI 6 — Sentimiento promedio del día ────────────────────────────
        rows6 = list(session.execute(
            "SELECT avg_sentiment FROM category_daily_stats "
            "WHERE stat_date=%s ALLOW FILTERING",
            (kpi_date,),
        ))
        sents = [r.avg_sentiment for r in rows6 if r.avg_sentiment is not None]
        kpi6 = sum(sents) / len(sents) if sents else 0.0
        _write_kpi(session, "avg_sentiment", ds, kpi6, f"Sentimiento medio del {ds}")

    finally:
        driver.close()

    logger.info("KPIs calculados para %s", ds)
