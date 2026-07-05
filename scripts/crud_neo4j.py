"""
CRUD Demo — Neo4j
Ejecutar: python scripts/crud_neo4j.py
"""
from __future__ import annotations
import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from neo4j import GraphDatabase
from src.common.config import NEO4J_URI, NEO4J_USER, NEO4J_PASSWORD

def sep(titulo: str):
    print(f"\n{'═'*55}")
    print(f"  {titulo}")
    print('═'*55)

def main():
    driver = GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASSWORD))

    with driver.session() as s:

        # Limpiar datos de prueba previos (idempotente)
        s.run("MATCH (n {test_node: true}) DETACH DELETE n")

        # ── CREATE ────────────────────────────────────────────────────
        sep("CREATE — Crear nodos y relaciones")

        # Crear usuario de prueba
        s.run("""
            CREATE (:User {
                user_id:      'TEST_USR_NEO4J',
                name:         'Jorge Test',
                review_count: 10,
                fans:         5,
                average_stars: 4.5,
                test_node:    true
            })
        """)
        print("  Nodo (:User {user_id: 'TEST_USR_NEO4J'}) creado")

        # Crear negocio de prueba
        s.run("""
            CREATE (:Business {
                business_id: 'TEST_BIZ_NEO4J',
                name:        'La Huaca UTEC',
                city:        'Philadelphia',
                stars:       4.8,
                review_count: 1,
                test_node:   true
            })
        """)
        print("  Nodo (:Business {business_id: 'TEST_BIZ_NEO4J'}) creado")

        # Crear categoría de prueba
        s.run("""
            MERGE (c:Category {name: 'Peruvian'})
        """)
        print("  Nodo (:Category {name: 'Peruvian'}) asegurado (MERGE)")

        # Crear relaciones
        s.run("""
            MATCH (u:User     {user_id:     'TEST_USR_NEO4J'})
            MATCH (b:Business {business_id: 'TEST_BIZ_NEO4J'})
            CREATE (u)-[:REVIEWED {
                review_id: 'TEST_REV_NEO4J',
                stars:     4.8,
                date:      '2024-06-15'
            }]->(b)
        """)
        print("  Relación (:User)-[:REVIEWED]->(Business) creada")

        s.run("""
            MATCH (b:Business {business_id: 'TEST_BIZ_NEO4J'})
            MATCH (c:Category {name: 'Peruvian'})
            MERGE (b)-[:IN_CATEGORY]->(c)
        """)
        print("  Relación (:Business)-[:IN_CATEGORY]->(:Category) creada")

        # Crear amistad con un usuario real
        real_user = s.run("""
            MATCH (u:User) WHERE u.user_id <> 'TEST_USR_NEO4J'
            RETURN u.user_id AS uid LIMIT 1
        """).single()

        if real_user:
            s.run("""
                MATCH (test:User {user_id: 'TEST_USR_NEO4J'})
                MATCH (real:User {user_id: $uid})
                MERGE (test)-[:FRIEND]->(real)
            """, uid=real_user["uid"])
            print(f"  Relación [:FRIEND] creada con usuario {real_user['uid'][:12]}...")

        # ── READ ──────────────────────────────────────────────────────
        sep("READ — Consultar nodos y relaciones")

        # Leer nodo usuario
        user = s.run("""
            MATCH (u:User {user_id: 'TEST_USR_NEO4J'})
            RETURN u.name AS nombre, u.fans AS fans, u.average_stars AS avg_stars
        """).single()
        print(f"  Usuario encontrado: {user['nombre']} | fans={user['fans']} | avg_stars={user['avg_stars']}")

        # Leer negocio con sus categorías
        biz_cats = s.run("""
            MATCH (b:Business {business_id: 'TEST_BIZ_NEO4J'})-[:IN_CATEGORY]->(c:Category)
            RETURN b.name AS negocio, b.stars AS stars, collect(c.name) AS categorias
        """).single()
        print(f"  Negocio: {biz_cats['negocio']} | ⭐ {biz_cats['stars']} | {biz_cats['categorias']}")

        # Leer relación REVIEWED
        rev = s.run("""
            MATCH (u:User {user_id: 'TEST_USR_NEO4J'})-[r:REVIEWED]->(b:Business)
            RETURN u.name AS quien, r.stars AS stars, b.name AS negocio, r.date AS fecha
        """).single()
        if rev:
            print(f"  Reseña: {rev['quien']} → {rev['negocio']} | ⭐{rev['stars']} | {rev['fecha']}")

        # KPI 4 — Top influencers
        print(f"\n  KPI 4 — Top 5 usuarios por número de amigos:")
        top_inf = s.run("""
            MATCH (u:User)-[:FRIEND]->(f:User)
            RETURN u.name AS nombre, u.user_id AS uid, count(f) AS amigos
            ORDER BY amigos DESC LIMIT 5
        """)
        for i, r in enumerate(top_inf, 1):
            nombre = r["nombre"] or r["uid"][:12]
            print(f"    {i}. {nombre} — {r['amigos']} amigos")

        # KPI 5 — Negocios más reseñados por la red
        print(f"\n  KPI 5 — Top 5 negocios reseñados por la red social:")
        top_biz = s.run("""
            MATCH (:User)-[:FRIEND]->(:User)-[:REVIEWED]->(b:Business)
            RETURN b.name AS negocio, count(*) AS menciones
            ORDER BY menciones DESC LIMIT 5
        """)
        for i, r in enumerate(top_biz, 1):
            print(f"    {i}. {r['negocio']} — {r['menciones']} menciones")

        # ── UPDATE ────────────────────────────────────────────────────
        sep("UPDATE — Actualizar propiedades de nodo")

        s.run("""
            MATCH (u:User {user_id: 'TEST_USR_NEO4J'})
            SET u.fans         = 99,
                u.review_count = 50,
                u.average_stars = 4.9
        """)
        print("  Propiedades actualizadas: fans=99 | review_count=50 | average_stars=4.9")

        verificado = s.run("""
            MATCH (u:User {user_id: 'TEST_USR_NEO4J'})
            RETURN u.fans AS fans, u.review_count AS reviews, u.average_stars AS avg
        """).single()
        print(f"  Verificación: fans={verificado['fans']} | reviews={verificado['reviews']} | avg={verificado['avg']}")

        # Actualizar propiedad de relación
        s.run("""
            MATCH (:User {user_id:'TEST_USR_NEO4J'})-[r:REVIEWED]->(:Business)
            SET r.stars = 5.0
        """)
        print("  Relación REVIEWED actualizada: stars=5.0")

        # ── DELETE ────────────────────────────────────────────────────
        sep("DELETE — Eliminar nodos y relaciones")

        # Primero eliminar solo la relación
        s.run("""
            MATCH (:User {user_id:'TEST_USR_NEO4J'})-[r:FRIEND]->()
            DELETE r
        """)
        print("  Relación [:FRIEND] del usuario de prueba eliminada")

        # Eliminar nodos de prueba con todas sus relaciones
        res = s.run("""
            MATCH (n {test_node: true})
            DETACH DELETE n
            RETURN count(n) AS eliminados
        """).single()
        print(f"  Nodos de prueba eliminados (DETACH DELETE): {res['eliminados']}")

        # Verificar
        existe = s.run("""
            MATCH (n {test_node: true}) RETURN count(n) AS n
        """).single()["n"]
        print(f"  Verificación post-delete: {existe} nodos restantes (esperado: 0)")

    driver.close()
    sep("CRUD Neo4j completado ✓")

if __name__ == "__main__":
    main()
