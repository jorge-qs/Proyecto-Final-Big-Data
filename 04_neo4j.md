# Rol 4 — Neo4j (relaciones entre entidades)

> El campo `friends` de Yelp es una **red social real**. Ahí está tu oro: consultas
> de relación que ninguna otra base hace bien.

---

## Objetivo
Modelar el grafo, construir nodos y relaciones, exponer funciones reutilizables para
Airflow y demostrar CRUD en Cypher. Cubres la **etapa 5 del DAG**.

## Tu alcance
- `src/loaders/neo4j_loader.py`
- `scripts/neo4j_constraints.cypher`
- La sección de red del dashboard (KPIs 4 y 5).

## Consumes
`data/staging/` (users con `friends`, reviews, businesses con `categories`).

---

## Modelo de grafo (justifícalo)

**Nodos**
```
(:User   {user_id, name, review_count, fans, yelping_since, average_stars})
(:Business {business_id, name, city, stars, review_count})
(:Category {name})
(:City   {name, state})
```

**Relaciones**
```
(:User)-[:FRIEND]->(:User)                       // desde user.friends
(:User)-[:REVIEWED {stars, date, review_id}]->(:Business)
(:Business)-[:IN_CATEGORY]->(:Category)
(:Business)-[:LOCATED_IN]->(:City)
```

**Constraints / índices** (`scripts/neo4j_constraints.cypher`):
```cypher
CREATE CONSTRAINT user_id IF NOT EXISTS
  FOR (u:User) REQUIRE u.user_id IS UNIQUE;
CREATE CONSTRAINT biz_id IF NOT EXISTS
  FOR (b:Business) REQUIRE b.business_id IS UNIQUE;
CREATE CONSTRAINT cat_name IF NOT EXISTS
  FOR (c:Category) REQUIRE c.name IS UNIQUE;
```

> Carga con `MERGE` (no `CREATE`) para idempotencia, y en **lotes** (`UNWIND $rows`)
> para velocidad. La lista `friends` puede ser enorme: limita o muestrea si hace falta.

---

## Funciones para Airflow
```python
# src/loaders/neo4j_loader.py
def upsert_users(rows: list[dict]) -> int: ...
def upsert_businesses(rows: list[dict]) -> int: ...
def build_friend_edges(rows: list[dict]) -> int: ...     # (:User)-[:FRIEND]->(:User)
def build_reviewed_edges(rows: list[dict]) -> int: ...
def build_category_edges(rows: list[dict]) -> int: ...
```
Patrón de carga por lotes:
```cypher
UNWIND $rows AS row
MERGE (u:User {user_id: row.user_id})
  SET u.review_count = row.review_count, u.fans = row.fans
```

---

## Consultas estrella (alimentan KPIs 4 y 5)
```cypher
// KPI 4 — usuario más influyente por nº de amigos (grado)
MATCH (u:User)-[:FRIEND]->(f:User)
RETURN u.user_id, count(f) AS amigos ORDER BY amigos DESC LIMIT 10;

// KPI 5 — negocios más reseñados por la red de un usuario
MATCH (:User {user_id:$id})-[:FRIEND]->(:User)-[:REVIEWED]->(b:Business)
RETURN b.name, count(*) AS reseñas_de_amigos ORDER BY reseñas_de_amigos DESC LIMIT 10;
```
(Opcional con GDS: `PageRank`/`betweenness` para una métrica de influencia más rica.)

---

## CRUD (demostrar en vivo)
- **Crear nodo:** `CREATE (:User {...})`.
- **Crear relación:** `MATCH ... MERGE (a)-[:FRIEND]->(b)`.
- **Actualizar propiedades:** `MATCH (u:User {user_id:...}) SET u.fans = ...`.
- **Eliminar:** `MATCH (n) ... DETACH DELETE n`.

## Qué documentar
- Diagrama del grafo (nodos y relaciones).
- Justificación de modelar `friends` como relación vs. propiedad.
- Las consultas estrella explicadas + capturas.
- Capturas de los 4 CRUD.

## Definición de "terminado"
- [ ] Constraints creados; carga por lotes idempotente con `MERGE`.
- [ ] Relaciones FRIEND, REVIEWED, IN_CATEGORY construidas.
- [ ] KPIs 4 y 5 consultables.
- [ ] CRUD demostrable.

## Prompt sugerido para tu agente
> "Soy el responsable de Neo4j en un proyecto Big Data sobre Yelp. Consumo `.jsonl`
> limpios de `data/staging/` (users con lista `friends`, reviews, businesses con
> categorías). Ayúdame a: (1) `scripts/neo4j_constraints.cypher`; (2)
> `src/loaders/neo4j_loader.py` con el driver oficial, carga por lotes con
> `UNWIND ... MERGE` idempotente para nodos User/Business/Category/City y relaciones
> FRIEND/REVIEWED/IN_CATEGORY/LOCATED_IN; (3) consultas Cypher para usuario más
> influyente (grado) y negocios más reseñados por la red de un usuario; (4) un script
> de demostración CRUD. Conexión por `NEO4J_URI`, usuario y password de entorno."
