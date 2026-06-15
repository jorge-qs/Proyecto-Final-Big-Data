# Proyecto Big Data — Arquitectura Multimodelo sobre Yelp Open Dataset

> **Fuente de verdad del equipo.** Léelo completo antes de tocar código. Cada
> integrante tiene además un `.md` propio con su rol detallado.

---

## 1. Idea en una frase

Tomamos **un solo dataset** (Yelp Open Dataset) y lo hacemos "viajar" por tres
bases de datos que cumplen propósitos distintos sobre **los mismos datos**:
MongoDB (documentos originales) → Cassandra (series temporales / agregados) →
Neo4j (relaciones). Apache Airflow orquesta ese viaje de forma programada, y un
dashboard en Streamlit muestra los KPIs.

**No hay 5 datasets ni 5 sistemas. Hay UNO solo.** El dato se extrae y se limpia
una vez, y de ahí fluye en cadena.

---

## 2. El dataset

**Yelp Open Dataset** (https://business.yelp.com/data/resources/open-dataset/).
Uso educativo. 5 archivos JSON, **un objeto JSON por línea**:

| Archivo | Contenido | ¿Para qué nos sirve? |
|---|---|---|
| `business.json` | Negocios: ubicación, `stars`, `review_count`, `attributes` (anidado y **variable**), `categories` (string separado por comas), `hours` | Documento rico → **MongoDB** |
| `review.json` | `review_id`, `user_id`, `business_id`, `stars` (1–5), `useful/funny/cool`, `text`, `date` (millones de filas) | Documento + serie temporal → **MongoDB** y **Cassandra** |
| `user.json` | `user_id`, `review_count`, `yelping_since`, **`friends` (lista de user_ids)**, `fans`, `average_stars` | Grafo social → **Neo4j** |
| `checkin.json` | `business_id`, `date` (lista de timestamps separados por coma) | Serie temporal → **Cassandra** |
| `tip.json` | `text`, `date`, `compliment_count`, `business_id`, `user_id` | Documento corto → **MongoDB** |

### Alcance acordado (para que sea manejable y reproducible)
- Filtramos a **una región** (ej. 1–2 áreas metropolitanas) o a una **categoría**
  (ej. restaurantes) para no cargar 5 GB.
- Objetivo: **≥ 50,000 reseñas** (el dataset las supera de sobra incluso filtrado).
- **Enriquecimiento** (lo que nos hace destacar): sentimiento del texto de reseñas
  (NLP simple), y particionado de reseñas por fecha para simular cargas incrementales.

### Justificación técnica (¡obligatoria en el informe!)
- **MongoDB** porque `attributes` varía de negocio a negocio (esquema flexible) y
  los documentos son ricos y anidados.
- **Cassandra** porque las reseñas y check-ins son eventos con marca de tiempo:
  escrituras masivas, consultas por fecha, agregados. Modelo *query-first*.
- **Neo4j** porque `friends` forma una red social real y queremos consultas de
  relación (influencers, "negocios reseñados por mis amigos").

---

## 3. Cómo nos repartimos (5 integrantes)

**4 dueños "verticales" + 1 de integración.** Cada uno es dueño de su carpeta en el
repo (minimiza conflictos de merge en GitHub).

| # | Rol | Archivo de instrucciones | Carpeta que posee |
|---|---|---|---|
| 1 | **Data Engineer** (datos + extracción + limpieza) | `01_data_engineer.md` | `src/extract/`, `src/transform/`, `src/common/` |
| 2 | **MongoDB** | `02_mongodb.md` | `src/loaders/mongo_loader.py`, `scripts/mongo_indexes.js` |
| 3 | **Cassandra** | `03_cassandra.md` | `src/loaders/cassandra_loader.py`, `scripts/cassandra_schema.cql` |
| 4 | **Neo4j** | `04_neo4j.md` | `src/loaders/neo4j_loader.py`, `scripts/neo4j_constraints.cypher` |
| 5 | **Airflow + Dashboard + DevOps** | `05_airflow_dashboard.md` | `dags/`, `dashboard/`, `docker-compose.yml`, `src/kpis/` |

> **La duda que más te preocupaba:** "¿cómo junto todo en Airflow sin rehacerlo?"
> **No se rehace nada.** Cada dueño escribe su carga como **funciones de Python
> reutilizables** (no como notebooks sueltos). El integrador solo las **importa** y
> las conecta como tareas del DAG. Ver §6.

---

## 4. El "contrato de datos" (la clave de trabajar en paralelo)

El Data Engineer produce el **dato limpio canónico** en `data/staging/`, y **todos
los demás consumen de ahí**. Mientras el contrato (nombres de campos y tipos) esté
acordado, cada uno puede avanzar sin esperar al otro.

Salida del Data Engineer — un archivo `.jsonl` por entidad (un objeto por línea),
**particionado por fecha** donde aplique:

```
data/staging/
├── businesses.jsonl
├── users.jsonl
├── reviews/dt=YYYY-MM-DD/part.jsonl     # particionado por fecha (incremental)
├── checkins/dt=YYYY-MM-DD/part.jsonl
└── tips.jsonl
```

El esquema exacto de cada campo vive en `src/common/schema.py` (lo define el Data
Engineer y es **inmutable sin avisar al equipo**). Esa es la frontera entre roles.

---

## 5. Estructura del repositorio (GitHub)

```
yelp-bigdata/
├── docker-compose.yml          # levanta Mongo + Cassandra + Neo4j + Airflow
├── .env.example                # credenciales de ejemplo (NO subir .env real)
├── requirements.txt
├── README.md                   # este archivo
├── data/
│   ├── raw/                    # JSON de Yelp (gitignored, muy pesado)
│   └── staging/                # salida limpia del Data Engineer
├── src/
│   ├── common/                 # contrato de datos: schema.py, config.py
│   ├── extract/                # descarga / lectura del raw
│   ├── transform/              # validación + limpieza + enriquecimiento
│   ├── loaders/                # mongo_loader.py, cassandra_loader.py, neo4j_loader.py
│   └── kpis/                   # compute_kpis.py
├── scripts/                    # creación de BD: .js, .cql, .cypher
├── dags/                       # yelp_pipeline_dag.py
├── dashboard/                  # app.py (Streamlit)
├── tests/
└── docs/report_assets/         # capturas y material del informe
```

### Reglas de GitHub
- `main` protegida. Cada uno trabaja en su rama: `feature/mongo`, `feature/cassandra`, etc.
- Pull Request + 1 revisión antes de mergear.
- Como cada uno posee su carpeta, los conflictos son casi nulos.
- `.gitignore`: `data/raw/`, `data/staging/`, `.env`, `__pycache__/`.

---

## 6. Cómo se conecta TODO en Airflow (sin rehacer nada)

Cada loader expone funciones limpias, p. ej.:

```python
# src/loaders/mongo_loader.py  (lo escribe el dueño de Mongo)
def load_businesses(records: list[dict]) -> int: ...
def load_reviews(records: list[dict]) -> int: ...
```

El DAG **solo importa y llama** esas funciones:

```python
# dags/yelp_pipeline_dag.py  (lo escribe el de Airflow)
from src.loaders import mongo_loader, cassandra_loader, neo4j_loader

PythonOperator(task_id="load_mongo",
               python_callable=mongo_loader.load_reviews, ...)
```

Flujo del DAG (las 6 etapas que pide el documento), programado `@daily`:

```
extract → validate_clean → load_mongo → ┬→ transform_load_cassandra →┐
                                         └→ build_load_neo4j ─────────┴→ generate_kpis
```

Cada corrida procesa **el "día lógico"** `{{ ds }}` → así simulamos actualizaciones
incrementales y cumplimos "ejecución programada".

---

## 7. Dashboard y KPIs

- **Tecnología:** Streamlit + Plotly (rápido para explorar + gráficos bonitos).
- Lee de las **3 bases**: Mongo (detalle/documentos), Cassandra (series/KPIs por
  fecha), Neo4j (red).
- **5 KPIs obligatorios** (cada uno con definición, fórmula, interpretación):

| # | KPI | Fórmula | Fuente |
|---|---|---|---|
| 1 | Reseñas procesadas por día | `COUNT(reviews where date=D)` | Cassandra |
| 2 | Crecimiento diario de reseñas (%) | `(C_D − C_{D-1}) / C_{D-1} × 100` | Cassandra |
| 3 | Calificación promedio por categoría | `AVG(stars) GROUP BY category` | Mongo / Cassandra |
| 4 | Usuario más influyente (centralidad) | `max grado en grafo FRIEND` (o PageRank) | Neo4j |
| 5 | Top negocios por reseñas de la red | negocios más reseñados por amigos | Neo4j + Mongo |
| 6 (extra) | Índice de sentimiento promedio | `AVG(sentiment) por categoría/fecha` | enriquecimiento NLP |

Detalle completo en `05_airflow_dashboard.md`.

---

## 8. Cronograma (faltan ~4 semanas → entrega 06/07/2026)

| Semana | Objetivo | Quién |
|---|---|---|
| **1** | Repo + `docker-compose` corriendo en las 5 máquinas. Data Engineer baja datos, hace EDA y **congela el contrato** (`schema.py`). | Todos / DE lidera |
| **2** | Cada dueño crea su esquema + loader + CRUD sobre una muestra. DE entrega `staging/` final. | DBs + DE |
| **3** | Integración en Airflow (cablear loaders al DAG), KPIs, esqueleto del dashboard. | Airflow lidera, apoyan DBs |
| **4** | Pulir dashboard, capturar evidencias, escribir informe PDF, grabar video (≤15 min), colchón. | Todos |

---

## 9. Entregables (del documento del profe)

- [ ] Informe técnico (PDF) — secciones en `docs/`
- [ ] Código fuente completo (este repo)
- [ ] Scripts de creación de BD (`scripts/`)
- [ ] DAG de Airflow (`dags/`)
- [ ] Dashboard funcional (`dashboard/`)
- [ ] Video demostrativo ≤ 15 min
- [ ] **Demostrar CRUD en vivo** durante la exposición (cada dueño el suyo)
