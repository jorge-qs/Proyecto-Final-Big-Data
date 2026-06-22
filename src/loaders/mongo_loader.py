"""
Carga de documentos Yelp en MongoDB.
Dueño: Rol 2 — MongoDB.
"""
from __future__ import annotations
import logging
from typing import Optional
from pymongo import MongoClient, UpdateOne
from pymongo.errors import BulkWriteError
from src.common.config import MONGO_URI

logger = logging.getLogger(__name__)
_client: Optional[MongoClient] = None


def _db():
    global _client
    if _client is None:
        _client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=10_000)
    return _client["yelp"]


def _bulk(collection: str, ops: list) -> int:
    """bulk_write tolerando errores de duplicado (upsert idempotente)."""
    if not ops:
        return 0
    try:
        r = _db()[collection].bulk_write(ops, ordered=False)
        return r.upserted_count + r.modified_count
    except BulkWriteError as exc:
        n_err = len(exc.details.get("writeErrors", []))
        logger.warning("%s: %d errores en bulk_write (ignorados)", collection, n_err)
        return exc.details.get("nUpserted", 0) + exc.details.get("nModified", 0)


def load_businesses(records: list[dict]) -> int:
    if not records:
        return 0
    ops = [
        UpdateOne({"_id": r["business_id"]}, {"$set": {**r, "_id": r["business_id"]}}, upsert=True)
        for r in records
    ]
    return _bulk("businesses", ops)


def load_reviews(records: list[dict]) -> int:
    if not records:
        return 0
    ops = [
        UpdateOne({"_id": r["review_id"]}, {"$set": {**r, "_id": r["review_id"]}}, upsert=True)
        for r in records
    ]
    return _bulk("reviews", ops)


def load_users(records: list[dict]) -> int:
    if not records:
        return 0
    ops = [
        UpdateOne({"_id": r["user_id"]}, {"$set": {**r, "_id": r["user_id"]}}, upsert=True)
        for r in records
    ]
    return _bulk("users", ops)


def load_tips(records: list[dict]) -> int:
    if not records:
        return 0
    ops = [
        UpdateOne(
            {"user_id": r["user_id"], "business_id": r["business_id"], "date": r["date"]},
            {"$set": r},
            upsert=True,
        )
        for r in records
    ]
    return _bulk("tips", ops)
