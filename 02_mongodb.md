# Rol 2 — MongoDB (documentos originales)

> Guardas el dato rico y semiestructurado tal como es. Tu superpoder: el esquema
> flexible de `attributes` (cada negocio trae claves distintas).

---

## Objetivo
Modelar y cargar los documentos de Yelp en MongoDB, exponer funciones de carga
reutilizables para Airflow, y demostrar CRUD. Cubres la **etapa 3 del DAG**.

## Tu alcance
- `src/loaders/mongo_loader.py` — funciones de carga (las llamará el DAG).
- `scripts/mongo_indexes.js` — creación de índices.
- Tu sección del dashboard (consultas de detalle/documento).

## Consumes
`data/staging/` (contrato del Data Engineer). **No leas el raw de Yelp directo.**

---

## Modelo de datos (justifícalo en el informe)

Base `yelp`. Colecciones:

| Colección | Documento | Por qué documento |
|---|---|---|
| `businesses` | negocio completo con `attributes` anidado y `categories` como array | `attributes` varía por negocio → esquema flexible |
| `reviews` | `review_id`, `user_id`, `business_id`, `stars`, `date`, `text`, votos, `sentiment` | texto libre + campos variables |
| `users` | perfil de usuario (sin la lista `friends`, eso va a Neo4j) | documento natural |
| `tips` | tip corto | documento corto |

Ejemplo `businesses`:
```json
{
  "_id": "tnhfDv5Il8EaGSXZGiuQGg",
  "name": "Garaje",
  "city": "Santa Barbara", "state": "CA",
  "stars": 4.5, "review_count": 137,
  "categories": ["Mexican", "Bars"],
  "attributes": { "RestaurantsTakeOut": true, "WiFi": "free" },
  "hours": { "Monday": "10:0-21:0" }
}
```

**Índices** (`scripts/mongo_indexes.js`): `business_id`, `user_id`, `date`,
`categories` (multikey). Usa `_id = business_id` para evitar duplicados (upsert).

---

## Funciones para Airflow (lo que importa el DAG)
```python
# src/loaders/mongo_loader.py
def load_businesses(records: list[dict]) -> int: ...
def load_reviews(records: list[dict]) -> int: ...   # upsert por review_id
def load_users(records: list[dict]) -> int: ...
def load_tips(records: list[dict]) -> int: ...
```
Usa **upsert / bulk_write** para que re-correr el DAG no duplique (idempotencia).

---

## CRUD (¡demostrar en vivo!)
Prepara un script o celda con los 4, sobre `reviews` o `businesses`:
- **Create:** `insert_one` de un documento de prueba.
- **Read:** `find` con filtro + proyección (ej. negocios `stars >= 4.5` en una ciudad).
- **Update:** `update_one` (ej. cambiar un atributo).
- **Delete:** `delete_one`.
Captura cada salida para el informe.

## Qué documentar
- Modelo de documentos + justificación de embeber vs. referenciar.
- Lista de índices y por qué cada uno.
- Capturas de los 4 CRUD.
- Conteo de documentos por colección.

## Definición de "terminado"
- [ ] `mongo_loader.py` con upsert idempotente y probado.
- [ ] Índices creados vía `scripts/mongo_indexes.js`.
- [ ] CRUD demostrable con capturas.
- [ ] Conectado a la sección de detalle del dashboard.

## Prompt sugerido para tu agente
> "Soy el responsable de MongoDB en un proyecto Big Data sobre Yelp. Consumo
> archivos `.jsonl` limpios de `data/staging/`. Ayúdame a: (1) escribir
> `src/loaders/mongo_loader.py` con funciones `load_businesses/reviews/users/tips`
> usando `pymongo` y `bulk_write` con upsert idempotente; (2) `scripts/mongo_indexes.js`
> con los índices; (3) un script de demostración CRUD (create/read/update/delete)
> sobre la colección reviews. Conexión por variable de entorno `MONGO_URI`. Incluye
> manejo de errores y un test básico."
