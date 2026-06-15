"""
Script de muestreo UNA SOLA VEZ.
Lee los JSON crudos de Yelp en modo streaming y escribe versiones
filtradas (por ciudad/estado) en data/raw/ con sufijo _sample.json.

Uso:
    python scripts/sample_raw.py --city Philadelphia --state PA
    python scripts/sample_raw.py --city "Las Vegas" --state NV
    python scripts/sample_raw.py --city Tampa --state FL

El resto del pipeline consume los archivos *_sample.json, NO los originales.
"""
import json
import argparse
import logging
from pathlib import Path

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)


def parse_args():
    p = argparse.ArgumentParser()
    p.add_argument("--city",  default="Philadelphia", help="Ciudad a filtrar")
    p.add_argument("--state", default="PA",           help="Código de estado (2 letras)")
    p.add_argument("--raw",   default="data/raw",     help="Carpeta con los JSON originales")
    p.add_argument("--max-reviews", type=int, default=200_000,
                   help="Límite de reseñas (para no cargar millones)")
    return p.parse_args()


def iter_jsonl(path: Path):
    """Lee un JSONL línea a línea sin cargar todo en RAM."""
    with open(path, encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if line:
                yield json.loads(line)


def write_jsonl(path: Path, records: list[dict]):
    with open(path, "w", encoding="utf-8") as f:
        for r in records:
            f.write(json.dumps(r, ensure_ascii=False) + "\n")
    log.info("  → %s (%d registros)", path.name, len(records))


def main():
    args = parse_args()
    raw = Path(args.raw)

    # ── 1. Businesses ─────────────────────────────────────────────────────
    log.info("Filtrando businesses → city='%s', state='%s'", args.city, args.state)
    businesses = []
    biz_ids = set()
    for r in iter_jsonl(raw / "business.json"):
        if r.get("city") == args.city and r.get("state") == args.state:
            businesses.append(r)
            biz_ids.add(r["business_id"])
    write_jsonl(raw / "business_sample.json", businesses)
    log.info("Negocios encontrados: %d", len(biz_ids))

    if not biz_ids:
        log.error("No se encontraron negocios. Verifica --city y --state.")
        return

    # ── 2. Reviews ────────────────────────────────────────────────────────
    log.info("Filtrando reviews (límite %d)...", args.max_reviews)
    reviews = []
    user_ids = set()
    for r in iter_jsonl(raw / "review.json"):
        if r.get("business_id") in biz_ids:
            reviews.append(r)
            user_ids.add(r["user_id"])
            if len(reviews) >= args.max_reviews:
                log.info("  Límite de %d reviews alcanzado.", args.max_reviews)
                break
    write_jsonl(raw / "review_sample.json", reviews)

    # ── 3. Users ──────────────────────────────────────────────────────────
    log.info("Filtrando users (%d user_ids de reviews)...", len(user_ids))
    users = []
    for r in iter_jsonl(raw / "user.json"):
        if r.get("user_id") in user_ids:
            users.append(r)
    write_jsonl(raw / "user_sample.json", users)

    # ── 4. Checkins ───────────────────────────────────────────────────────
    log.info("Filtrando checkins...")
    checkins = []
    for r in iter_jsonl(raw / "checkin.json"):
        if r.get("business_id") in biz_ids:
            checkins.append(r)
    write_jsonl(raw / "checkin_sample.json", checkins)

    # ── 5. Tips ───────────────────────────────────────────────────────────
    log.info("Filtrando tips...")
    tips = []
    for r in iter_jsonl(raw / "tip.json"):
        if r.get("business_id") in biz_ids:
            tips.append(r)
    write_jsonl(raw / "tip_sample.json", tips)

    # ── Resumen ───────────────────────────────────────────────────────────
    log.info("=" * 50)
    log.info("MUESTREO COMPLETADO para %s, %s", args.city, args.state)
    log.info("  Businesses : %d", len(businesses))
    log.info("  Reviews    : %d", len(reviews))
    log.info("  Users      : %d", len(users))
    log.info("  Checkins   : %d", len(checkins))
    log.info("  Tips       : %d", len(tips))
    log.info("Ahora edita src/common/config.py para apuntar a los *_sample.json")


if __name__ == "__main__":
    main()
