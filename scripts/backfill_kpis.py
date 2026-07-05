"""
Backfill de KPIs históricos.

Calcula los 9 KPIs dinámicos para cada fecha con datos en daily_review_counts.
Todos los KPIs provienen de Cassandra — no se requiere Neo4j por fecha.

Uso:
    PYTHONPATH=. python scripts/backfill_kpis.py                      # todas las fechas
    PYTHONPATH=. python scripts/backfill_kpis.py --since 2019-01-01   # desde una fecha
    PYTHONPATH=. python scripts/backfill_kpis.py --dry-run             # listar fechas sin calcular
"""
from __future__ import annotations
import argparse
import logging
import time
from datetime import date

from src.common.config import CASSANDRA_HOSTS, CASSANDRA_PORT, CASSANDRA_KEYSPACE
from cassandra.cluster import Cluster
from src.kpis import compute_kpis

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)


def _get_all_dates(since: date = None) -> list:
    cluster = Cluster(CASSANDRA_HOSTS, port=CASSANDRA_PORT)
    session = cluster.connect(CASSANDRA_KEYSPACE)
    rows  = session.execute("SELECT review_date FROM daily_review_counts")
    dates = sorted(set(str(r.review_date) for r in rows))
    cluster.shutdown()
    if since:
        dates = [d for d in dates if d >= since.isoformat()]
    return dates


def main():
    parser = argparse.ArgumentParser(description="Backfill de KPIs históricos")
    parser.add_argument("--since",   type=str, default=None,
                        help="Procesar solo desde YYYY-MM-DD (inclusive)")
    parser.add_argument("--dry-run", action="store_true",
                        help="Solo listar fechas a procesar, sin calcular KPIs")
    args = parser.parse_args()

    since = date.fromisoformat(args.since) if args.since else None
    dates = _get_all_dates(since)
    log.info("Fechas a procesar: %d", len(dates))

    if args.dry_run:
        for d in dates[:20]:
            print(d)
        if len(dates) > 20:
            print(f"... y {len(dates) - 20} fechas más")
        return

    t0 = time.perf_counter()
    ok, errors = 0, 0

    for ds in dates:
        try:
            compute_kpis.run(ds)
            ok += 1
            if ok % 500 == 0:
                elapsed   = time.perf_counter() - t0
                rate      = ok / elapsed
                remaining = (len(dates) - ok) / rate if rate > 0 else 0
                log.info(
                    "Progreso %d/%d — %.0f fechas/s — ~%.0fs restantes",
                    ok, len(dates), rate, remaining,
                )
        except Exception as e:
            errors += 1
            log.error("Error en %s: %s", ds, e)

    elapsed = time.perf_counter() - t0
    log.info(
        "Backfill completado en %.1fs — %d fechas OK | %d errores",
        elapsed, ok, errors,
    )


if __name__ == "__main__":
    main()
