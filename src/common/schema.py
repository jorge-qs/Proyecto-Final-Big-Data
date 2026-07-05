"""
Contrato de datos canónico del proyecto.
INMUTABLE: cualquier cambio debe comunicarse a todo el equipo.
Dueño: Data Engineer (Rol 1).
"""
from __future__ import annotations
from datetime import date, datetime
from typing import Dict, List, Optional
from pydantic import BaseModel, Field


class Business(BaseModel):
    business_id: str
    name: str
    city: str
    state: str
    stars: float
    review_count: int
    categories: List[str] = Field(default_factory=list)
    attributes: Dict = Field(default_factory=dict)
    hours: Dict = Field(default_factory=dict)
    latitude: Optional[float] = None
    longitude: Optional[float] = None
    is_open: Optional[int] = None


class Review(BaseModel):
    review_id: str
    user_id: str
    business_id: str
    stars: float
    useful: int
    funny: int
    cool: int
    text: str
    date: date
    sentiment: Optional[float] = None   # -1.0 a 1.0 (VADER)


class User(BaseModel):
    user_id: str
    name: str
    review_count: int
    yelping_since: date
    fans: int
    average_stars: float
    friends: List[str] = Field(default_factory=list)  # lista de user_ids → Neo4j


class Checkin(BaseModel):
    business_id: str
    checkin_ts: datetime   # explotado: un objeto por timestamp


class Tip(BaseModel):
    text: str
    date: date
    compliment_count: int
    business_id: str
    user_id: str
