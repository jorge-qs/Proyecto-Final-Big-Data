"""
CRUD Demo — Cassandra
Ejecutar: python scripts/crud_cassandra.py
"""
from __future__ import annotations
import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from datetime import date, datetime
from cassandra.cluster import Cluster
from src.common.config import CASSANDRA_HOSTS, CASSANDRA_PORT, CASSANDRA_KEYSPACE

def sep(titulo: str):
    print(f"\n{'═'*55}")
    print(f"  {titulo}")
    print('═'*55)

def main():
    session = Cluster(CASSANDRA_HOSTS, port=CASSANDRA_PORT).connect(CASSANDRA_KEYSPACE)

    TEST_BIZ  = "TEST_BIZ_CASSANDRA"
    TEST_DATE = date(2024, 6, 15)
    TEST_REV  = "TEST_REV_CASSANDRA"

    # ── CREATE (INSERT) ───────────────────────────────────────────────
    sep("CREATE — Insertar filas")

    # Reseña por negocio
    session.execute("""
        INSERT INTO reviews_by_business_date
        (business_id, review_date, review_id, stars, user_id)
        VALUES (%s, %s, %s, %s, %s)
    """, (TEST_BIZ, TEST_DATE, TEST_REV, 4.5, "TEST_USR_001"))
    print(f"  Reseña insertada en reviews_by_business_date")
    print(f"    business_id={TEST_BIZ} | date={TEST_DATE} | stars=4.5")

    # Conteo diario
    session.execute("""
        INSERT INTO daily_review_counts (year_month, review_date, total)
        VALUES (%s, %s, %s)
    """, ("2024-06", TEST_DATE, 42))
    print(f"  Conteo diario insertado: 2024-06-15 → 42 reseñas")

    # Stats por categoría
    session.execute("""
        INSERT INTO category_daily_stats
        (category, stat_date, review_count, avg_stars, avg_sentiment)
        VALUES (%s, %s, %s, %s, %s)
    """, ("Peruvian", TEST_DATE, 10, 4.7, 0.82))
    print(f"  Stats de categoría insertados: Peruvian | avg_stars=4.7")

    # Check-in
    session.execute("""
        INSERT INTO checkins_by_business (business_id, checkin_ts)
        VALUES (%s, %s)
    """, (TEST_BIZ, datetime(2024, 6, 15, 20, 30, 0)))
    print(f"  Check-in insertado: {TEST_BIZ} @ 2024-06-15 20:30")

    # ── READ (SELECT) ─────────────────────────────────────────────────
    sep("READ — Consultar por clave de partición")

    print(f"  Reseñas de {TEST_BIZ}:")
    rows = session.execute("""
        SELECT review_id, review_date, stars, user_id
        FROM reviews_by_business_date
        WHERE business_id = %s
    """, (TEST_BIZ,))
    for r in rows:
        print(f"    review_id={r.review_id} | date={r.review_date} | stars={r.stars}")

    print(f"\n  Conteos diarios de 2024-06:")
    rows2 = session.execute("""
        SELECT review_date, total FROM daily_review_counts
        WHERE year_month = %s
    """, ("2024-06",))
    for r in rows2:
        print(f"    {r.review_date} → {r.total} reseñas")

    print(f"\n  Stats de categoría 'Peruvian':")
    rows3 = session.execute("""
        SELECT stat_date, review_count, avg_stars, avg_sentiment
        FROM category_daily_stats WHERE category = %s
    """, ("Peruvian",))
    for r in rows3:
        print(f"    {r.stat_date} | count={r.review_count} | stars={r.avg_stars:.2f} | sentiment={r.avg_sentiment:.2f}")

    # ── UPDATE ────────────────────────────────────────────────────────
    sep("UPDATE — Actualizar valor existente")

    # En Cassandra UPDATE = upsert sobre la misma clave
    session.execute("""
        UPDATE daily_review_counts SET total = %s
        WHERE year_month = %s AND review_date = %s
    """, (99, "2024-06", TEST_DATE))
    print(f"  Conteo actualizado: 2024-06-15 → 99 reseñas")

    verificado = session.execute("""
        SELECT total FROM daily_review_counts
        WHERE year_month = %s AND review_date = %s
    """, ("2024-06", TEST_DATE)).one()
    print(f"  Verificación: total = {verificado.total}")

    session.execute("""
        UPDATE category_daily_stats
        SET avg_stars = %s, avg_sentiment = %s
        WHERE category = %s AND stat_date = %s
    """, (4.9, 0.95, "Peruvian", TEST_DATE))
    print(f"  Stats Peruvian actualizados: avg_stars=4.9 | sentiment=0.95")

    # ── DELETE ────────────────────────────────────────────────────────
    sep("DELETE — Eliminar filas de prueba")

    session.execute("""
        DELETE FROM reviews_by_business_date
        WHERE business_id = %s AND review_date = %s AND review_id = %s
    """, (TEST_BIZ, TEST_DATE, TEST_REV))
    print(f"  Reseña eliminada de reviews_by_business_date")

    session.execute("""
        DELETE FROM daily_review_counts
        WHERE year_month = %s AND review_date = %s
    """, ("2024-06", TEST_DATE))
    print(f"  Conteo diario eliminado")

    session.execute("""
        DELETE FROM category_daily_stats
        WHERE category = %s AND stat_date = %s
    """, ("Peruvian", TEST_DATE))
    print(f"  Stats de Peruvian eliminados")

    session.execute("""
        DELETE FROM checkins_by_business
        WHERE business_id = %s AND checkin_ts = %s
    """, (TEST_BIZ, datetime(2024, 6, 15, 20, 30, 0)))
    print(f"  Check-in eliminado")

    # Verificar
    check = session.execute("""
        SELECT * FROM reviews_by_business_date WHERE business_id = %s
    """, (TEST_BIZ,)).all()
    print(f"\n  Verificación post-delete: {len(check)} filas (esperado: 0)")

    sep("CRUD Cassandra completado ✓")

if __name__ == "__main__":
    main()
