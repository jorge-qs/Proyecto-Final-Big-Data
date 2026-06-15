"""
Lectura de los archivos JSON crudos del Yelp Open Dataset.
Dueño: Data Engineer (Rol 1).
"""
import json
from pathlib import Path
from typing import Iterator
from src.common.config import RAW_DATA_PATH


def iter_json_lines(filename: str) -> Iterator[dict]:
    """Lee un archivo JSONL línea a línea (lazy, sin cargar todo en RAM).
    Prefiere la versión _sample si existe (evita cargar los archivos de 5GB)."""
    base = filename.replace(".json", "")
    raw = Path(RAW_DATA_PATH)
    path = raw / f"{base}_sample.json" if (raw / f"{base}_sample.json").exists() else raw / filename
    with open(path, encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if line:
                yield json.loads(line)


def read_businesses() -> Iterator[dict]:
    return iter_json_lines("business.json")

def read_reviews() -> Iterator[dict]:
    return iter_json_lines("review.json")

def read_users() -> Iterator[dict]:
    return iter_json_lines("user.json")

def read_checkins() -> Iterator[dict]:
    return iter_json_lines("checkin.json")

def read_tips() -> Iterator[dict]:
    return iter_json_lines("tip.json")
