"""
Calcula los KPIs del proyecto y los escribe en kpi_results (Cassandra).
Dueño: Rol 5 — Airflow + Dashboard.

KPIs dinámicos (varían por fecha, fuente: Cassandra):
    1. reviews_per_day            — volumen diario de reseñas procesadas
    2. daily_growth_pct           — crecimiento vs día anterior (%)
    3. top_category_avg_stars     — rating de la categoría mejor calificada ese día (mín 3 reseñas)
    4. active_categories          — nº de categorías con al menos 1 reseña ese día
    5. avg_stars_of_day           — rating promedio ponderado por volumen (índice global de calidad)
    6. avg_sentiment              — sentimiento VADER promedio del día
    7. top_category_quality_score — categoría con mejor score stars×sentiment×volume
    8. pct_high_rated_categories  — % categorías con avg_stars ≥ 4.0 ese día
    9. pct_positive_categories    — % categorías con sentimiento > 0.1

Métricas estructurales de la red (Neo4j) — calculadas una sola vez en el dashboard,
no se repiten por fecha en kpi_results.
"""
import logging
from datetime import date, timedelta
from src.common.config import (
    CASSANDRA_KEYSPACE, CASSANDRA_HOSTS, CASSANDRA_PORT,
)
from cassandra.cluster import Cluster

logger = logging.getLogger(__name__)

_MIN_REVIEWS = 3

_cass_session = None


def _cassandra():
    global _cass_session
    if _cass_session is None:
        cluster = Cluster(CASSANDRA_HOSTS, port=CASSANDRA_PORT)
        _cass_session = cluster.connect(CASSANDRA_KEYSPACE)
    return _cass_session


def _write_kpi(session, kpi_name: str, kpi_date: str, value: float, detail: str = "") -> None:
    session.execute(
        "INSERT INTO kpi_results (kpi_name, kpi_date, value, detail) VALUES (%s, %s, %s, %s)",
        (kpi_name, kpi_date, value, detail),
    )
    logger.info("KPI %-35s [%s] = %10.4f  | %s", kpi_name, kpi_date, value, detail)


def run(ds: str, precomputed: dict = None) -> None:
    """
    Calcula los 9 KPIs dinámicos para el día ds y los persiste en Cassandra.
    precomputed: ignorado, se mantiene por compatibilidad con llamadas anteriores.
    """
    session = _cassandra()
    kpi_date = date.fromisoformat(ds)
    year_month = ds[:7]

    # ── KPI 1 — Reseñas procesadas por día ──────────────────────────────────
    row = session.execute(
        "SELECT total FROM daily_review_counts WHERE year_month=%s AND review_date=%s",
        (year_month, kpi_date),
    ).one()
    kpi1 = float(row.total) if row else 0.0
    _write_kpi(session, "reviews_per_day", ds, kpi1, f"Reseñas procesadas el {ds}")

    # ── KPI 2 — Crecimiento diario (%) ──────────────────────────────────────
    prev_ds  = (kpi_date - timedelta(days=1)).isoformat()
    row_prev = session.execute(
        "SELECT total FROM daily_review_counts WHERE year_month=%s AND review_date=%s",
        (prev_ds[:7], date.fromisoformat(prev_ds)),
    ).one()
    prev = float(row_prev.total) if row_prev else 0.0
    kpi2 = ((kpi1 - prev) / prev * 100) if prev > 0 else 0.0
    _write_kpi(session, "daily_growth_pct", ds, kpi2, f"Crecimiento vs {prev_ds}")

    # ── Datos base: todas las categorías del día (reutilizadas en KPIs 3–9) ─
    rows_cat = list(session.execute(
        "SELECT category, avg_stars, avg_sentiment, review_count FROM category_daily_stats "
        "WHERE stat_date=%s ALLOW FILTERING",
        (kpi_date,),
    ))

    # ── KPI 3 — Categoría mejor calificada ese día (mín. _MIN_REVIEWS) ──────
    qualified3 = [r for r in rows_cat if (r.review_count or 0) >= _MIN_REVIEWS]
    best3  = max(qualified3, key=lambda r: r.avg_stars or 0.0, default=None)
    kpi3   = float(best3.avg_stars) if best3 else 0.0
    detail3 = f"{best3.category} ({best3.review_count} reseñas)" if best3 else "sin datos"
    _write_kpi(session, "top_category_avg_stars", ds, kpi3, detail3)

    # ── KPI 4 — Número de categorías activas ese día ─────────────────────────
    kpi4 = float(len(rows_cat))
    _write_kpi(session, "active_categories", ds, kpi4,
               f"{int(kpi4)} categorías con al menos 1 reseña el {ds}")

    # ── KPI 5 — Rating promedio ponderado por volumen ────────────────────────
    total_vol = sum(r.review_count or 0 for r in rows_cat)
    if total_vol > 0:
        kpi5    = sum((r.avg_stars or 0.0) * (r.review_count or 0) for r in rows_cat) / total_vol
        detail5 = f"Promedio ponderado ({total_vol} reseñas en total)"
    else:
        kpi5, detail5 = 0.0, "sin datos"
    _write_kpi(session, "avg_stars_of_day", ds, kpi5, detail5)

    # ── KPI 6 — Sentimiento promedio del día ─────────────────────────────────
    sents = [r.avg_sentiment for r in rows_cat if r.avg_sentiment is not None]
    kpi6  = sum(sents) / len(sents) if sents else 0.0
    _write_kpi(session, "avg_sentiment", ds, kpi6, f"Sentimiento medio del {ds}")

    # ── KPI 7 — Categoría con mejor score calidad×volumen del día ────────────
    if rows_cat:
        def _score(r):
            stars = r.avg_stars or 0.0
            sent  = max((r.avg_sentiment or 0.0) + 1.0, 0.1)
            vol   = r.review_count or 0
            return stars * sent * vol

        best7   = max(rows_cat, key=_score, default=None)
        kpi7    = _score(best7) if best7 else 0.0
        detail7 = (
            f"{best7.category} — {best7.avg_stars:.2f}★ "
            f"sent={best7.avg_sentiment:.3f} n={best7.review_count}"
        ) if best7 else "sin datos"
    else:
        kpi7, detail7 = 0.0, "sin datos"
    _write_kpi(session, "top_category_quality_score", ds, kpi7, detail7)

    # ── KPI 8 — % categorías con rating ≥ 4.0 ese día ───────────────────────
    if rows_cat:
        high_rated = sum(1 for r in rows_cat if (r.avg_stars or 0.0) >= 4.0)
        kpi8    = high_rated / len(rows_cat) * 100
        detail8 = f"{high_rated} de {len(rows_cat)} categorías con ≥4.0★"
    else:
        kpi8, detail8 = 0.0, "sin datos"
    _write_kpi(session, "pct_high_rated_categories", ds, kpi8, detail8)

    # ── KPI 9 — % de categorías con sentimiento positivo (VADER > 0.1) ───────
    if rows_cat:
        positivas = sum(1 for r in rows_cat if (r.avg_sentiment or 0.0) > 0.1)
        kpi9      = positivas / len(rows_cat) * 100
        detail9   = f"{positivas} de {len(rows_cat)} categorías con sentimiento > 0.1"
    else:
        kpi9, detail9 = 0.0, "sin datos"
    _write_kpi(session, "pct_positive_categories", ds, kpi9, detail9)

    logger.info("KPIs calculados para %s", ds)
