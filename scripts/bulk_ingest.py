"""
Carga histórica masiva de reviews del Yelp Open Dataset.
Úsalo UNA VEZ para poblar las bases con todos los datos del *_sample.json.
Complementa el DAG (que hace cargas incrementales diarias).

Uso:
    python scripts/bulk_ingest.py
    python scripts/bulk_ingest.py --limit 50000           # subset para pruebas
    python scripts/bulk_ingest.py --skip-mongo            # solo Cassandra + Neo4j
    python scripts/bulk_ingest.py --reserve-dates 2019-01-28,2019-01-29
        # Deja esas fechas sin cargar en Cassandra/KPIs para usarlas como
        # demo del DAG Airflow (MongoDB y Neo4j sí se cargan siempre).
        # Puedes agregar más fechas separadas por coma.
"""
from __future__ import annotations
import json
import logging
import time
import argparse
from collections import defaultdict
from pathlib import Path

from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer
from src.common.config import RAW_DATA_PATH, STAGING_PATH
from src.loaders import mongo_loader, cassandra_loader, neo4j_loader

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

CHUNK = 1_000
_vader = SentimentIntensityAnalyzer()


def parse_args():
    p = argparse.ArgumentParser()
    p.add_argument("--limit",          type=int, default=0,  help="Máx reviews a procesar (0 = todos)")
    p.add_argument("--skip-mongo",     action="store_true",  help="Omitir carga a MongoDB")
    p.add_argument("--skip-cassandra", action="store_true",  help="Omitir carga a Cassandra")
    p.add_argument("--skip-neo4j",     action="store_true",  help="Omitir carga a Neo4j")
    p.add_argument(
        "--reserve-dates",
        default="2019-01-28,2019-01-29",
        help=(
            "Fechas separadas por coma a excluir de Cassandra (quedan limpias para "
            "demo del DAG). Pasar '' para no reservar ninguna. "
            "Default: 2019-01-28,2019-01-29"
        ),
    )
    return p.parse_args()


def _iter_jsonl(path: Path):
    if not path.exists():
        log.warning("Archivo no encontrado: %s", path)
        return
    with open(path, encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if line:
                yield json.loads(line)


def _chunked(it, size):
    chunk = []
    for item in it:
        chunk.append(item)
        if len(chunk) >= size:
            yield chunk
            chunk = []
    if chunk:
        yield chunk


def _resolve_reviews_path() -> Path:
    raw = Path(RAW_DATA_PATH)
    for suffix in ("review_demo.json", "review_sample.json", "review.json"):
        p = raw / suffix
        if p.exists():
            log.info("Usando archivo de reviews: %s", p)
            return p
    raise FileNotFoundError("No se encontró ningún archivo de reviews en data/raw/")


def load_all_reviews(args) -> list[dict]:
    """Lee, limpia y enriquece todas las reviews. Devuelve lista de dicts canónicos."""
    path = _resolve_reviews_path()
    reviews: list[dict] = []
    skipped = 0
    t0 = time.perf_counter()

    with open(path, encoding="utf-8") as f:
        for i, line in enumerate(f):
            if args.limit and len(reviews) >= args.limit:
                break
            line = line.strip()
            if not line:
                continue
            try:
                r = json.loads(line)
                reviews.append({
                    "review_id":   r["review_id"],
                    "user_id":     r["user_id"],
                    "business_id": r["business_id"],
                    "stars":       float(r["stars"]),
                    "useful":      int(r.get("useful", 0)),
                    "funny":       int(r.get("funny", 0)),
                    "cool":        int(r.get("cool", 0)),
                    "text":        r.get("text", ""),
                    "date":        (r.get("date") or "")[:10],
                    "sentiment":   _vader.polarity_scores(r.get("text", ""))["compound"],
                })
                if len(reviews) % 10_000 == 0:
                    log.info("  reviews leídas: %d (%.1fs)", len(reviews), time.perf_counter() - t0)
            except Exception as e:
                skipped += 1
                if skipped <= 5:
                    log.warning("review descartada (línea %d): %s", i, e)

    log.info("Reviews cargadas: %d | descartadas: %d | %.1fs", len(reviews), skipped, time.perf_counter() - t0)
    return reviews


def ingest_mongo(reviews: list[dict]):
    log.info("=== MongoDB: cargando %d reviews ===", len(reviews))
    t0 = time.perf_counter()
    total = 0
    for chunk in _chunked(reviews, CHUNK):
        total += mongo_loader.load_reviews(chunk)
    log.info("MongoDB: %d operaciones en %.1fs", total, time.perf_counter() - t0)


def ingest_cassandra(reviews: list[dict], biz_cats: dict, reserve: set[str] | None = None):
    reserve = reserve or set()
    log.info("=== Cassandra: procesando %d reviews por fecha ===", len(reviews))
    if reserve:
        log.info("  Fechas reservadas (excluidas): %s", sorted(reserve))
    t0 = time.perf_counter()

    by_date: dict[str, list] = defaultdict(list)
    for r in reviews:
        if r.get("date"):
            by_date[r["date"]].append(r)

    log.info("Fechas distintas en el sample: %d", len(by_date))
    loaded, skipped_reserved = 0, 0
    for ds, day_reviews in sorted(by_date.items()):
        if ds in reserve:
            log.info("  [RESERVADA] %s — %d reviews omitidas de Cassandra", ds, len(day_reviews))
            skipped_reserved += 1
            continue
        try:
            cassandra_loader.upsert_reviews_by_business(day_reviews)
            cassandra_loader.upsert_daily_counts(ds, day_reviews)
            cassandra_loader.upsert_category_stats(ds, day_reviews, biz_cats)
            loaded += 1
        except Exception as e:
            log.error("Cassandra error en %s: %s", ds, e)

    log.info(
        "Cassandra: %d fechas cargadas | %d reservadas para demo | %.1fs",
        loaded, skipped_reserved, time.perf_counter() - t0,
    )


def ingest_neo4j(reviews: list[dict]):
    log.info("=== Neo4j: construyendo %d aristas REVIEWED ===", len(reviews))
    t0 = time.perf_counter()
    total = 0
    for chunk in _chunked(reviews, CHUNK):
        total += neo4j_loader.build_reviewed_edges(chunk)
    log.info("Neo4j: %d aristas REVIEWED en %.1fs", total, time.perf_counter() - t0)


def main():
    args = parse_args()
    reserve: set[str] = (
        {d.strip() for d in args.reserve_dates.split(",") if d.strip()}
        if args.reserve_dates else set()
    )

    log.info("=== BULK INGEST iniciado ===")
    if reserve:
        log.info("Fechas reservadas para demo Airflow: %s", sorted(reserve))
        log.info("  → Esas fechas se cargan en MongoDB y Neo4j pero NO en Cassandra/KPIs.")
        log.info("  → Úsalas con: bash scripts/demo_airflow.sh --date YYYY-MM-DD")

    # Cargar mapa de categorías desde staging (ya limpio)
    biz_cats: dict[str, list] = {}
    staging = Path(STAGING_PATH)
    for b in _iter_jsonl(staging / "businesses.jsonl"):
        biz_cats[b["business_id"]] = b.get("categories", [])
    log.info("biz_cats cargado: %d negocios", len(biz_cats))

    if not biz_cats:
        log.warning(
            "biz_cats vacío — puede que data/staging/businesses.jsonl no exista. "
            "Los category_stats de Cassandra no tendrán categorías. "
            "Ejecuta el DAG al menos una vez (extract + validate_clean) para generarlo."
        )

    reviews = load_all_reviews(args)
    if not reviews:
        log.error("Sin reviews para cargar. Verifica data/raw/.")
        return

    if not args.skip_mongo:
        ingest_mongo(reviews)

    if not args.skip_cassandra:
        ingest_cassandra(reviews, biz_cats, reserve)

    if not args.skip_neo4j:
        ingest_neo4j(reviews)

    log.info("=== BULK INGEST completado ===")
    if reserve:
        log.info("")
        log.info("Fechas disponibles para demo del DAG:")
        for d in sorted(reserve):
            log.info("  bash scripts/demo_airflow.sh --date %s", d)


if __name__ == "__main__":
    main()
