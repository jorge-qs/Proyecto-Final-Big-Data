"""
Carga de documentos Yelp en MongoDB.
Dueño: Rol 2 — MongoDB.
"""
from __future__ import annotations
from typing import Optional
from pymongo import MongoClient, UpdateOne
from src.common.config import MONGO_URI

_client: Optional[MongoClient] = None

def _db():
    global _client
    if _client is None:
        _client = MongoClient(MONGO_URI)
    return _client["yelp"]


def load_businesses(records: list[dict]) -> int:
    if not records:
        return 0
    ops = [UpdateOne({"_id": r["business_id"]}, {"$set": {**r, "_id": r["business_id"]}}, upsert=True) for r in records]
    result = _db()["businesses"].bulk_write(ops, ordered=False)
    return result.upserted_count + result.modified_count


def load_reviews(records: list[dict]) -> int:
    if not records:
        return 0
    ops = [UpdateOne({"_id": r["review_id"]}, {"$set": {**r, "_id": r["review_id"]}}, upsert=True) for r in records]
    result = _db()["reviews"].bulk_write(ops, ordered=False)
    return result.upserted_count + result.modified_count


def load_users(records: list[dict]) -> int:
    if not records:
        return 0
    ops = [UpdateOne({"_id": r["user_id"]}, {"$set": {**r, "_id": r["user_id"]}}, upsert=True) for r in records]
    result = _db()["users"].bulk_write(ops, ordered=False)
    return result.upserted_count + result.modified_count


def load_tips(records: list[dict]) -> int:
    if not records:
        return 0
    ops = [UpdateOne(
        {"user_id": r["user_id"], "business_id": r["business_id"], "date": r["date"]},
        {"$set": r}, upsert=True
    ) for r in records]
    result = _db()["tips"].bulk_write(ops, ordered=False)
    return result.upserted_count + result.modified_count
