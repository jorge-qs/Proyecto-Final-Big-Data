"""
CRUD Demo — MongoDB
Ejecutar: python scripts/crud_mongo.py
"""
from __future__ import annotations
import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from pymongo import MongoClient
from src.common.config import MONGO_URI

def sep(titulo: str):
    print(f"\n{'═'*55}")
    print(f"  {titulo}")
    print('═'*55)

def main():
    db = MongoClient(MONGO_URI)["yelp"]

    # ── CREATE ────────────────────────────────────────────────────────
    sep("CREATE — Insertar documento de prueba")
    doc = {
        "_id":          "TEST_BIZ_001",
        "business_id":  "TEST_BIZ_001",
        "name":         "La Huaca UTEC",
        "city":         "Philadelphia",
        "state":        "PA",
        "stars":        4.8,
        "review_count": 1,
        "categories":   ["Peruvian", "Restaurants"],
        "attributes":   {"RestaurantsTakeOut": True, "WiFi": "free"},
        "hours":        {"Monday": "11:0-22:0"},
    }
    db.businesses.delete_one({"_id": "TEST_BIZ_001"})   # idempotente
    result = db.businesses.insert_one(doc)
    print(f"  Documento insertado con _id: {result.inserted_id}")

    review_doc = {
        "_id":         "TEST_REV_001",
        "review_id":   "TEST_REV_001",
        "user_id":     "TEST_USR_001",
        "business_id": "TEST_BIZ_001",
        "stars":       5.0,
        "text":        "Increíble ceviche, el mejor de Philly!",
        "date":        "2024-06-15",
        "useful":      3, "funny": 0, "cool": 2,
        "sentiment":   0.92,
    }
    db.reviews.delete_one({"_id": "TEST_REV_001"})
    db.reviews.insert_one(review_doc)
    print(f"  Reseña insertada con _id: TEST_REV_001")

    # ── READ ──────────────────────────────────────────────────────────
    sep("READ — Consultar documentos")

    negocio = db.businesses.find_one(
        {"_id": "TEST_BIZ_001"},
        {"name": 1, "stars": 1, "categories": 1, "city": 1}
    )
    print(f"  Negocio encontrado: {negocio['name']} | "
          f"Stars: {negocio['stars']} | City: {negocio['city']}")

    print(f"\n  Top 5 negocios con stars >= 4.8 en Philadelphia:")
    top = db.businesses.find(
        {"stars": {"$gte": 4.8}, "city": "Philadelphia"},
        {"name": 1, "stars": 1, "review_count": 1}
    ).sort("review_count", -1).limit(5)
    for i, b in enumerate(top, 1):
        print(f"    {i}. {b['name']} | ⭐ {b['stars']} | {b['review_count']} reseñas")

    print(f"\n  Reseñas de TEST_BIZ_001:")
    revs = list(db.reviews.find({"business_id": "TEST_BIZ_001"}))
    for r in revs:
        print(f"    ⭐ {r['stars']} | {r['text'][:50]}... | sentiment={r['sentiment']}")

    # ── UPDATE ────────────────────────────────────────────────────────
    sep("UPDATE — Actualizar documento")

    res = db.businesses.update_one(
        {"_id": "TEST_BIZ_001"},
        {"$set": {"stars": 5.0, "review_count": 2},
         "$push": {"categories": "Latin American"}}
    )
    print(f"  Documentos modificados: {res.modified_count}")
    actualizado = db.businesses.find_one({"_id": "TEST_BIZ_001"}, {"stars": 1, "categories": 1})
    print(f"  Nuevo rating: {actualizado['stars']}")
    print(f"  Categorías: {actualizado['categories']}")

    # ── DELETE ────────────────────────────────────────────────────────
    sep("DELETE — Eliminar documentos de prueba")

    r1 = db.businesses.delete_one({"_id": "TEST_BIZ_001"})
    r2 = db.reviews.delete_one({"_id": "TEST_REV_001"})
    print(f"  Negocio eliminado:  {r1.deleted_count} documento(s)")
    print(f"  Reseña eliminada:   {r2.deleted_count} documento(s)")

    # Verificar que ya no existe
    existe = db.businesses.find_one({"_id": "TEST_BIZ_001"})
    print(f"  Verificación post-delete: {existe}")

    sep("CRUD MongoDB completado ✓")

if __name__ == "__main__":
    main()
