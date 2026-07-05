"""
Validación, limpieza y enriquecimiento del Yelp Open Dataset.
Dueño: Data Engineer (Rol 1).
"""
from __future__ import annotations
import json
import logging
import time
from pathlib import Path
from datetime import datetime
from typing import Iterator
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer
from src.common.config import STAGING_PATH
from src.common.schema import Business, Review, User, Checkin, Tip
from src.extract.reader import (
    read_businesses, read_reviews, read_users, read_checkins, read_tips
)

logger = logging.getLogger(__name__)
_vader = SentimentIntensityAnalyzer()


def _sentiment(text: str) -> float:
    return _vader.polarity_scores(text)["compound"]


def _write_jsonl_stream(path: Path, records_iter: Iterator[dict]) -> int:
    """Escribe un iterador de dicts en JSONL sin cargar todo en RAM. Devuelve el conteo."""
    path.parent.mkdir(parents=True, exist_ok=True)
    count = 0
    with open(path, "w", encoding="utf-8") as f:
        for r in records_iter:
            f.write(json.dumps(r, default=str) + "\n")
            count += 1
    return count


def _gen_businesses() -> Iterator[dict]:
    seen: set[str] = set()
    for raw in read_businesses():
        bid = raw.get("business_id")
        if not bid or bid in seen:
            continue
        try:
            cats = [c.strip() for c in (raw.get("categories") or "").split(",") if c.strip()]
            obj = Business(
                business_id=bid,
                name=raw.get("name", ""),
                city=raw.get("city", ""),
                state=raw.get("state", ""),
                stars=float(raw.get("stars", 0)),
                review_count=int(raw.get("review_count", 0)),
                categories=cats,
                attributes=raw.get("attributes") or {},
                hours=raw.get("hours") or {},
                latitude=raw.get("latitude"),
                longitude=raw.get("longitude"),
                is_open=raw.get("is_open"),
            )
            seen.add(bid)
            yield obj.model_dump()
        except Exception as e:
            logger.warning("business descartado %s: %s", bid, e)


def _gen_users() -> Iterator[dict]:
    seen: set[str] = set()
    for raw in read_users():
        uid = raw.get("user_id")
        if not uid or uid in seen:
            continue
        try:
            friends = [
                f.strip() for f in (raw.get("friends") or "").split(",")
                if f.strip() and f.strip() != "None"
            ]
            obj = User(
                user_id=uid,
                name=raw.get("name", ""),
                review_count=int(raw.get("review_count", 0)),
                yelping_since=raw["yelping_since"][:10],
                fans=int(raw.get("fans", 0)),
                average_stars=float(raw.get("average_stars", 0)),
                friends=friends,
            )
            seen.add(uid)
            yield obj.model_dump()
        except Exception as e:
            logger.warning("user descartado %s: %s", uid, e)


def _gen_reviews(ds: str) -> Iterator[dict]:
    seen: set[str] = set()
    for raw in read_reviews():
        raw_date = (raw.get("date") or "")[:10]
        rid = raw.get("review_id")
        if raw_date != ds or not rid or rid in seen:
            continue
        try:
            obj = Review(
                review_id=rid,
                user_id=raw["user_id"],
                business_id=raw["business_id"],
                stars=float(raw["stars"]),
                useful=int(raw.get("useful", 0)),
                funny=int(raw.get("funny", 0)),
                cool=int(raw.get("cool", 0)),
                text=raw.get("text", ""),
                date=raw_date,
                sentiment=_sentiment(raw.get("text", "")),
            )
            seen.add(rid)
            yield obj.model_dump()
        except Exception as e:
            logger.warning("review descartada %s: %s", rid, e)


def _gen_checkins(ds: str) -> Iterator[dict]:
    for raw in read_checkins():
        for ts_str in (raw.get("date") or "").split(","):
            ts_str = ts_str.strip()
            if not ts_str:
                continue
            try:
                ts = datetime.strptime(ts_str, "%Y-%m-%d %H:%M:%S")
                if ts.strftime("%Y-%m-%d") != ds:
                    continue
                obj = Checkin(business_id=raw["business_id"], checkin_ts=ts)
                yield obj.model_dump()
            except Exception as e:
                logger.warning("checkin descartado: %s", e)


def _gen_tips() -> Iterator[dict]:
    seen: set[tuple] = set()
    for raw in read_tips():
        key = (raw.get("user_id"), raw.get("business_id"), (raw.get("date") or "")[:10])
        if key in seen:
            continue
        try:
            obj = Tip(
                text=raw.get("text", ""),
                date=(raw.get("date") or "")[:10],
                compliment_count=int(raw.get("compliment_count", 0)),
                business_id=raw["business_id"],
                user_id=raw["user_id"],
            )
            seen.add(key)
            yield obj.model_dump()
        except Exception as e:
            logger.warning("tip descartado: %s", e)


def build_staging_for_date(ds: str) -> dict:
    """
    Limpia y escribe el slice de reviews/checkins para la fecha ds (YYYY-MM-DD).
    Idempotente: re-correr sobreescribe los mismos archivos.
    Usa escritura en streaming para evitar OOM con el dataset completo.
    Devuelve conteos por entidad.
    """
    counts: dict[str, int] = {}
    staging = Path(STAGING_PATH)

    t0 = time.perf_counter()
    counts["businesses"] = _write_jsonl_stream(staging / "businesses.jsonl", _gen_businesses())
    logger.info("businesses: %d en %.1fs", counts["businesses"], time.perf_counter() - t0)

    t0 = time.perf_counter()
    counts["users"] = _write_jsonl_stream(staging / "users.jsonl", _gen_users())
    logger.info("users: %d en %.1fs", counts["users"], time.perf_counter() - t0)

    t0 = time.perf_counter()
    counts["reviews"] = _write_jsonl_stream(
        staging / f"reviews/dt={ds}/part.jsonl", _gen_reviews(ds)
    )
    logger.info("reviews[%s]: %d en %.1fs", ds, counts["reviews"], time.perf_counter() - t0)

    t0 = time.perf_counter()
    counts["checkins"] = _write_jsonl_stream(
        staging / f"checkins/dt={ds}/part.jsonl", _gen_checkins(ds)
    )
    logger.info("checkins[%s]: %d en %.1fs", ds, counts["checkins"], time.perf_counter() - t0)

    t0 = time.perf_counter()
    counts["tips"] = _write_jsonl_stream(staging / "tips.jsonl", _gen_tips())
    logger.info("tips: %d en %.1fs", counts["tips"], time.perf_counter() - t0)

    logger.info("Staging para %s completado: %s", ds, counts)
    return counts
