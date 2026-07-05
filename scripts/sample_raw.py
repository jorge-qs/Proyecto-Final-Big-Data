"""
Script de muestreo del Yelp Open Dataset.

Modos:
  --sample  (por defecto) filtra por ciudad/estado y escribe *_sample.json
  --demo    genera *_demo.json con un subset pequeño para la presentación:
              · 500 negocios con mayor review_count en la ciudad
              · 5 000 reseñas más recientes de esos negocios
              · usuarios involucrados en esas reseñas
              · checkins y tips de esos negocios

Uso:
    python scripts/sample_raw.py --city Philadelphia --state PA
    python scripts/sample_raw.py --city Philadelphia --state PA --demo
    python scripts/sample_raw.py --city "Las Vegas" --state NV
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
                   help="Límite de reseñas en modo sample (default: 200 000)")
    p.add_argument("--demo", action="store_true",
                   help="Genera *_demo.json en lugar de *_sample.json (subset para demo)")
    p.add_argument("--demo-businesses", type=int, default=500,
                   help="Negocios en modo demo (default: 500)")
    p.add_argument("--demo-reviews", type=int, default=5_000,
                   help="Reseñas en modo demo (default: 5 000)")
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


# ── Modo sample (completo filtrado por ciudad) ─────────────────────────────

def run_sample(args):
    raw = Path(args.raw)
    suffix = "_sample"

    log.info("=== MODO SAMPLE: city='%s', state='%s' ===", args.city, args.state)

    businesses, biz_ids = [], set()
    for r in iter_jsonl(raw / "business.json"):
        if r.get("city") == args.city and r.get("state") == args.state:
            businesses.append(r)
            biz_ids.add(r["business_id"])
    write_jsonl(raw / f"business{suffix}.json", businesses)
    log.info("Negocios: %d", len(biz_ids))

    if not biz_ids:
        log.error("No se encontraron negocios. Verifica --city y --state.")
        return

    reviews, user_ids = [], set()
    for r in iter_jsonl(raw / "review.json"):
        if r.get("business_id") in biz_ids:
            reviews.append(r)
            user_ids.add(r["user_id"])
            if len(reviews) >= args.max_reviews:
                log.info("  Límite de %d reviews alcanzado.", args.max_reviews)
                break
    write_jsonl(raw / f"review{suffix}.json", reviews)

    users = [r for r in iter_jsonl(raw / "user.json") if r.get("user_id") in user_ids]
    write_jsonl(raw / f"user{suffix}.json", users)

    checkins = [r for r in iter_jsonl(raw / "checkin.json") if r.get("business_id") in biz_ids]
    write_jsonl(raw / f"checkin{suffix}.json", checkins)

    tips = [r for r in iter_jsonl(raw / "tip.json") if r.get("business_id") in biz_ids]
    write_jsonl(raw / f"tip{suffix}.json", tips)

    log.info("=" * 55)
    log.info("SAMPLE completado para %s, %s", args.city, args.state)
    log.info("  Businesses : %d", len(businesses))
    log.info("  Reviews    : %d", len(reviews))
    log.info("  Users      : %d", len(users))
    log.info("  Checkins   : %d", len(checkins))
    log.info("  Tips       : %d", len(tips))


# ── Modo demo (subset pequeño para presentación) ───────────────────────────

def run_demo(args):
    raw = Path(args.raw)
    suffix = "_demo"

    log.info("=== MODO DEMO: city='%s', state='%s' ===", args.city, args.state)
    log.info("    %d negocios · %d reseñas", args.demo_businesses, args.demo_reviews)

    # 1. Top N negocios por review_count en la ciudad
    all_biz = [
        r for r in iter_jsonl(raw / "business.json")
        if r.get("city") == args.city and r.get("state") == args.state
    ]
    if not all_biz:
        log.error("No se encontraron negocios. Verifica --city y --state.")
        return

    all_biz.sort(key=lambda r: int(r.get("review_count") or 0), reverse=True)
    businesses = all_biz[: args.demo_businesses]
    biz_ids = {r["business_id"] for r in businesses}
    write_jsonl(raw / f"business{suffix}.json", businesses)
    log.info("Negocios seleccionados: %d (de %d en %s)", len(businesses), len(all_biz), args.city)

    # 2. Las N reseñas más recientes de esos negocios
    candidate_reviews = []
    for r in iter_jsonl(raw / "review.json"):
        if r.get("business_id") in biz_ids:
            candidate_reviews.append(r)

    candidate_reviews.sort(key=lambda r: r.get("date") or "", reverse=True)
    reviews = candidate_reviews[: args.demo_reviews]
    user_ids = {r["user_id"] for r in reviews}
    write_jsonl(raw / f"review{suffix}.json", reviews)

    # 3. Usuarios de esas reseñas
    users = [r for r in iter_jsonl(raw / "user.json") if r.get("user_id") in user_ids]
    write_jsonl(raw / f"user{suffix}.json", users)

    # 4. Checkins de esos negocios
    checkins = [r for r in iter_jsonl(raw / "checkin.json") if r.get("business_id") in biz_ids]
    write_jsonl(raw / f"checkin{suffix}.json", checkins)

    # 5. Tips de esos negocios
    tips = [r for r in iter_jsonl(raw / "tip.json") if r.get("business_id") in biz_ids]
    write_jsonl(raw / f"tip{suffix}.json", tips)

    log.info("=" * 55)
    log.info("DEMO completado para %s, %s", args.city, args.state)
    log.info("  Businesses : %d", len(businesses))
    log.info("  Reviews    : %d (más recientes)", len(reviews))
    log.info("  Users      : %d", len(users))
    log.info("  Checkins   : %d", len(checkins))
    log.info("  Tips       : %d", len(tips))
    log.info("")
    log.info("Los archivos *_demo.json serán detectados automáticamente")
    log.info("por src/extract/reader.py (tienen prioridad sobre *_sample.json).")
    log.info("Para volver al modo completo: elimina los *_demo.json de data/raw/")


def main():
    args = parse_args()
    if args.demo:
        run_demo(args)
    else:
        run_sample(args)


if __name__ == "__main__":
    main()
