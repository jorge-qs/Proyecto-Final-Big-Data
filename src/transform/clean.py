"""
Validación, limpieza y enriquecimiento del Yelp Open Dataset.
Dueño: Data Engineer (Rol 1).
"""
from __future__ import annotations
import json
import logging
from pathlib import Path
from datetime import datetime
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


def _write_jsonl(path: Path, records: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        for r in records:
            f.write(json.dumps(r, default=str) + "\n")


def build_staging_for_date(ds: str) -> dict:
    """
    Limpia y escribe el slice de reviews/checkins para la fecha ds (YYYY-MM-DD).
    Idempotente: re-correr sobreescribe el mismo archivo.
    Devuelve conteos por entidad.
    """
    counts = {"businesses": 0, "users": 0, "reviews": 0, "checkins": 0, "tips": 0}

    # --- Businesses (se cargan completos, no por día) ---
    businesses, seen_biz = [], set()
    for raw in read_businesses():
        if raw.get("business_id") in seen_biz:
            continue
        try:
            cats = [c.strip() for c in (raw.get("categories") or "").split(",") if c.strip()]
            obj = Business(
                business_id=raw["business_id"],
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
            businesses.append(obj.model_dump())
            seen_biz.add(obj.business_id)
        except Exception as e:
            logger.warning("business descartado: %s", e)
    _write_jsonl(Path(STAGING_PATH) / "businesses.jsonl", businesses)
    counts["businesses"] = len(businesses)

    # --- Users ---
    users, seen_usr = [], set()
    for raw in read_users():
        if raw.get("user_id") in seen_usr:
            continue
        try:
            friends = [f.strip() for f in (raw.get("friends") or "").split(",") if f.strip() and f.strip() != "None"]
            obj = User(
                user_id=raw["user_id"],
                name=raw.get("name", ""),
                review_count=int(raw.get("review_count", 0)),
                yelping_since=raw["yelping_since"][:10],
                fans=int(raw.get("fans", 0)),
                average_stars=float(raw.get("average_stars", 0)),
                friends=friends,
            )
            users.append(obj.model_dump())
            seen_usr.add(obj.user_id)
        except Exception as e:
            logger.warning("user descartado: %s", e)
    _write_jsonl(Path(STAGING_PATH) / "users.jsonl", users)
    counts["users"] = len(users)

    # --- Reviews del día ds ---
    reviews, seen_rev = [], set()
    for raw in read_reviews():
        raw_date = (raw.get("date") or "")[:10]
        if raw_date != ds or raw.get("review_id") in seen_rev:
            continue
        try:
            obj = Review(
                review_id=raw["review_id"],
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
            reviews.append(obj.model_dump())
            seen_rev.add(obj.review_id)
        except Exception as e:
            logger.warning("review descartada: %s", e)
    _write_jsonl(Path(STAGING_PATH) / f"reviews/dt={ds}/part.jsonl", reviews)
    counts["reviews"] = len(reviews)

    # --- Checkins del día ds ---
    checkins = []
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
                checkins.append(obj.model_dump())
            except Exception as e:
                logger.warning("checkin descartado: %s", e)
    _write_jsonl(Path(STAGING_PATH) / f"checkins/dt={ds}/part.jsonl", checkins)
    counts["checkins"] = len(checkins)

    # --- Tips ---
    tips, seen_tip = [], set()
    for raw in read_tips():
        key = (raw.get("user_id"), raw.get("business_id"), (raw.get("date") or "")[:10])
        if key in seen_tip:
            continue
        try:
            obj = Tip(
                text=raw.get("text", ""),
                date=(raw.get("date") or "")[:10],
                compliment_count=int(raw.get("compliment_count", 0)),
                business_id=raw["business_id"],
                user_id=raw["user_id"],
            )
            tips.append(obj.model_dump())
            seen_tip.add(key)
        except Exception as e:
            logger.warning("tip descartado: %s", e)
    _write_jsonl(Path(STAGING_PATH) / "tips.jsonl", tips)
    counts["tips"] = len(tips)

    logger.info("Staging para %s completado: %s", ds, counts)
    return counts
