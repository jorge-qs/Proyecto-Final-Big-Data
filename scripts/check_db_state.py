"""
Verifica el estado de las tres bases de datos del proyecto.
Muestra conteos de todas las colecciones/tablas y recomienda qué hacer.

Uso:
    PYTHONPATH=. .venv/bin/python3 scripts/check_db_state.py
"""
from __future__ import annotations
import sys
from src.common.config import (
    MONGO_URI, CASSANDRA_HOSTS, CASSANDRA_PORT, CASSANDRA_KEYSPACE,
    NEO4J_URI, NEO4J_USER, NEO4J_PASSWORD,
)

GREEN  = "\033[0;32m"
YELLOW = "\033[1;33m"
RED    = "\033[0;31m"
CYAN   = "\033[0;36m"
BOLD   = "\033[1m"
NC     = "\033[0m"

issues: list[str] = []


def _ok(msg):   print(f"  {GREEN}✓{NC}  {msg}")
def _warn(msg): print(f"  {YELLOW}~{NC}  {msg}"); issues.append(msg)
def _err(msg):  print(f"  {RED}✗{NC}  {msg}"); issues.append(msg)


# ── MongoDB ────────────────────────────────────────────────────────────────

def check_mongo():
    print(f"\n{BOLD}{CYAN}MongoDB{NC}")
    try:
        from pymongo import MongoClient
        client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=5_000)
        db = client["yelp"]

        counts = {
            "businesses": db.businesses.count_documents({}),
            "reviews":    db.reviews.count_documents({}),
            "users":      db.users.count_documents({}),
            "tips":       db.tips.count_documents({}),
        }

        for col, n in counts.items():
            if n == 0:
                _warn(f"{col}: VACÍO")
            else:
                _ok(f"{col}: {n:,}")

        client.close()
        return counts
    except Exception as e:
        _err(f"No se pudo conectar: {e}")
        return {}


# ── Cassandra ─────────────────────────────────────────────────────────────

def check_cassandra():
    print(f"\n{BOLD}{CYAN}Cassandra  (keyspace: {CASSANDRA_KEYSPACE}){NC}")
    try:
        from cassandra.cluster import Cluster
        cluster = Cluster(CASSANDRA_HOSTS, port=CASSANDRA_PORT)
        session = cluster.connect(CASSANDRA_KEYSPACE)

        # Contar fechas distintas en daily_review_counts
        dates = list(session.execute("SELECT review_date FROM daily_review_counts"))
        n_dates = len(set(str(r.review_date) for r in dates))

        # Filas en cada tabla
        n_cat   = len(list(session.execute("SELECT category FROM category_daily_stats LIMIT 10000")))
        n_rev   = len(list(session.execute("SELECT business_id FROM reviews_by_business_date LIMIT 10000")))
        n_kpi   = len(list(session.execute("SELECT kpi_name FROM kpi_results LIMIT 10000")))
        n_ck    = len(list(session.execute("SELECT business_id FROM checkins_by_business LIMIT 10000")))

        rows = {
            "daily_review_counts (fechas)": n_dates,
            "category_daily_stats":         n_cat,
            "reviews_by_business_date":     n_rev,
            "kpi_results":                  n_kpi,
            "checkins_by_business":         n_ck,
        }

        for table, n in rows.items():
            if n == 0:
                _warn(f"{table}: VACÍO")
            else:
                _ok(f"{table}: {n:,} filas")

        # Mostrar rango de fechas con datos
        if n_dates > 0:
            date_strs = sorted(set(str(r.review_date) for r in dates))
            print(f"       fechas: {date_strs[0]} → {date_strs[-1]}  ({n_dates} días)")

        cluster.shutdown()
        return rows
    except Exception as e:
        _err(f"No se pudo conectar: {e}")
        return {}


# ── Neo4j ─────────────────────────────────────────────────────────────────

def check_neo4j():
    print(f"\n{BOLD}{CYAN}Neo4j{NC}")
    try:
        from neo4j import GraphDatabase
        driver = GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASSWORD))

        counts = {}
        with driver.session() as s:
            counts["User"]     = s.run("MATCH (n:User)     RETURN count(n) AS c").single()["c"]
            counts["Business"] = s.run("MATCH (n:Business) RETURN count(n) AS c").single()["c"]
            counts["Category"] = s.run("MATCH (n:Category) RETURN count(n) AS c").single()["c"]
            counts["FRIEND"]   = s.run("MATCH ()-[r:FRIEND]->()   RETURN count(r) AS c").single()["c"]
            counts["REVIEWED"] = s.run("MATCH ()-[r:REVIEWED]->() RETURN count(r) AS c").single()["c"]

        for label, n in counts.items():
            marker = "arista" if label in ("FRIEND", "REVIEWED") else "nodo"
            if n == 0:
                _warn(f"{label} ({marker}s): VACÍO")
            else:
                _ok(f"{label} ({marker}s): {n:,}")

        driver.close()
        return counts
    except Exception as e:
        _err(f"No se pudo conectar: {e}")
        return {}


# ── Main ──────────────────────────────────────────────────────────────────

def main():
    print(f"\n{BOLD}{'='*55}")
    print("   Estado de las bases de datos — Yelp Big Data")
    print(f"{'='*55}{NC}")

    mongo_c  = check_mongo()
    cass_c   = check_cassandra()
    neo4j_c  = check_neo4j()

    print(f"\n{BOLD}{'='*55}{NC}")

    if not issues:
        print(f"\n{GREEN}{BOLD}✓  Todas las BDs tienen datos. Listo para el demo.{NC}")
        print(f"\n   Airflow UI → http://localhost:8080  (admin/admin)")
        print(f"   Dashboard  → http://localhost:8501")
        print(f"\n   Para el video: bash scripts/demo_airflow.sh")
    else:
        print(f"\n{YELLOW}{BOLD}⚠  Se detectaron {len(issues)} tabla(s) vacía(s).{NC}")
        if not mongo_c or mongo_c.get("reviews", 0) == 0:
            print(f"\n   Carga inicial requerida:")
            print(f"   {CYAN}PYTHONPATH=. .venv/bin/python3 scripts/bulk_ingest.py{NC}")
            print(f"   {CYAN}PYTHONPATH=. .venv/bin/python3 scripts/backfill_kpis.py{NC}")
        else:
            print(f"\n   Solo faltan KPIs o tablas Cassandra. Ejecuta:")
            print(f"   {CYAN}PYTHONPATH=. .venv/bin/python3 scripts/backfill_kpis.py{NC}")


if __name__ == "__main__":
    main()
    sys.exit(0)
