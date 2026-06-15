"""
Carga del grafo social Yelp en Neo4j.
Dueño: Rol 4 — Neo4j.
"""
from __future__ import annotations
from neo4j import GraphDatabase
from src.common.config import NEO4J_URI, NEO4J_USER, NEO4J_PASSWORD

_driver = None

def _get_driver():
    global _driver
    if _driver is None:
        _driver = GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASSWORD))
    return _driver

BATCH = 500


def _run_batch(query: str, rows: list[dict]) -> int:
    with _get_driver().session() as session:
        for i in range(0, len(rows), BATCH):
            session.run(query, rows=rows[i:i + BATCH])
    return len(rows)


def upsert_users(rows: list[dict]) -> int:
    q = """
    UNWIND $rows AS row
    MERGE (u:User {user_id: row.user_id})
      SET u.name = row.name,
          u.review_count = row.review_count,
          u.fans = row.fans,
          u.average_stars = row.average_stars
    """
    return _run_batch(q, rows)


def upsert_businesses(rows: list[dict]) -> int:
    q = """
    UNWIND $rows AS row
    MERGE (b:Business {business_id: row.business_id})
      SET b.name = row.name, b.city = row.city,
          b.stars = row.stars, b.review_count = row.review_count
    MERGE (c:City {name: row.city, state: row.state})
    MERGE (b)-[:LOCATED_IN]->(c)
    """
    return _run_batch(q, rows)


def build_friend_edges(rows: list[dict]) -> int:
    q = """
    UNWIND $rows AS row
    MATCH (u:User {user_id: row.user_id})
    UNWIND row.friends AS fid
    MATCH (f:User {user_id: fid})
    MERGE (u)-[:FRIEND]->(f)
    """
    return _run_batch(q, rows)


def build_reviewed_edges(rows: list[dict]) -> int:
    q = """
    UNWIND $rows AS row
    MATCH (u:User {user_id: row.user_id})
    MATCH (b:Business {business_id: row.business_id})
    MERGE (u)-[r:REVIEWED {review_id: row.review_id}]->(b)
      SET r.stars = row.stars, r.date = row.date
    """
    return _run_batch(q, rows)


def build_category_edges(rows: list[dict]) -> int:
    q = """
    UNWIND $rows AS row
    MATCH (b:Business {business_id: row.business_id})
    UNWIND row.categories AS cat
    MERGE (c:Category {name: cat})
    MERGE (b)-[:IN_CATEGORY]->(c)
    """
    return _run_batch(q, rows)
