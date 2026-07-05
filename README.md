# Arquitectura Multimodelo sobre el Yelp Open Dataset

**UTEC — Big Data | Proyecto Grupal II | Julio 2026**

> Un solo dataset. Tres bases de datos. Un pipeline orquestado con Airflow. Un dashboard en tiempo real.

---

## Tecnologías

![Python](https://img.shields.io/badge/Python-3.11-blue?logo=python)
![Docker](https://img.shields.io/badge/Docker-Compose-2496ED?logo=docker)
![Airflow](https://img.shields.io/badge/Apache%20Airflow-2.8-017CEE?logo=apache-airflow)
![MongoDB](https://img.shields.io/badge/MongoDB-7.0-47A248?logo=mongodb)
![Cassandra](https://img.shields.io/badge/Cassandra-4.1-1287B1?logo=apache-cassandra)
![Neo4j](https://img.shields.io/badge/Neo4j-5.x-008CC1?logo=neo4j)
![Streamlit](https://img.shields.io/badge/Streamlit-1.x-FF4B4B?logo=streamlit)

---

## Descripción

Procesamos el **Yelp Open Dataset** (Philadelphia, PA) a través de tres bases de datos especializadas orquestadas por Apache Airflow:

| Base de datos | Modelo | Caso de uso |
|---|---|---|
| **MongoDB** | Documentos | Negocios con atributos variables, reviews, users, tips |
| **Cassandra** | Series temporales | Conteos diarios, estadísticas por categoría, KPIs |
| **Neo4j** | Grafo social | Red de amistades, influencia, traversals |

---

## Arquitectura del pipeline

```
RAW JSON (Yelp)
      │
      ▼
  [ extract ]
      │
      ▼
[ validate_clean ]  ← VADER NLP aplicado al texto de reviews
      │
      ├─────────────────────┬──────────────────────────┐
      │                     │                          │
      ▼                     ▼                          ▼
[ load_mongo ]  [ transform_load_cassandra ]  [ build_load_neo4j ]
      │                     │                          │
      └─────────────────────┴──────────────────────────┘
                            │
                    [ generate_kpis ]
                            │
                            ▼
                   [ Dashboard Streamlit ]
```

El DAG `yelp_pipeline` corre `@daily` — cada ejecución procesa el día lógico `{{ ds }}` de forma **incremental e idempotente**. Los tres loaders (Mongo, Cassandra, Neo4j) corren en paralelo tras la limpieza.

---

## Dataset

**Yelp Open Dataset** — filtrado a Philadelphia, PA.

| Entidad | Registros | Destino |
|---|---|---|
| Businesses | 14,567 | MongoDB + Neo4j |
| Reviews | 200,000 | MongoDB + Cassandra |
| Users | 98,138 | Neo4j (grafo social) |
| Tips | 115,910 | MongoDB |
| Check-ins | — | Cassandra |

Rango temporal: **2005-06-27 → 2022-01-19** (5,572 fechas distintas con reviews reales).

---

## Estructura del repositorio

```
.
├── docker-compose.yml          # Todos los servicios: MongoDB, Cassandra, Neo4j, Airflow
├── start.sh / stop.sh          # Levantar y apagar el entorno
├── requirements.txt
├── dags/
│   └── yelp_pipeline_dag.py    # DAG principal de Airflow
├── src/
│   ├── common/                 # config.py, schema.py
│   ├── extract/                # Verificación de archivos raw
│   ├── transform/              # Limpieza, validación, VADER NLP
│   ├── loaders/                # mongo_loader.py · cassandra_loader.py · neo4j_loader.py
│   └── kpis/                   # compute_kpis.py — 9 KPIs dinámicos
├── scripts/
│   ├── cassandra_schema.cql    # DDL del keyspace yelp_analytics
│   ├── neo4j_constraints.cypher
│   ├── mongo_indexes.js
│   ├── demo_airflow.sh         # Demo del pipeline en vivo (triggea el DAG + polling)
│   └── trigger_date.py         # Triggea el DAG para una fecha específica
├── dashboard/
│   └── app.py                  # Streamlit — 4 tabs: MongoDB · Cassandra · Neo4j · KPIs
├── data/
│   ├── raw/                    # JSON de Yelp (gitignored)
│   └── staging/                # JSONL particionado por fecha (gitignored)
└── informe/
    └── informe.pdf             # Informe técnico completo
```

---

## Cómo ejecutar

### 1. Levantar el entorno

```bash
bash start.sh
```

Levanta todos los servicios vía Docker Compose:
- MongoDB → `localhost:27017`
- Cassandra → `localhost:9042`
- Neo4j → `localhost:7474` / `7687`
- Airflow → `localhost:8080` (admin / admin)

### 2. Abrir el dashboard

```bash
PYTHONPATH=. .venv/bin/python3 -m streamlit run dashboard/app.py
```

→ [http://localhost:8501](http://localhost:8501)

### 3. Demo del pipeline Airflow

```bash
# Script de demo con polling en tiempo real
bash scripts/demo_airflow.sh

# O triggear una fecha específica
PYTHONPATH=. .venv/bin/python3 scripts/trigger_date.py 2019-01-27
```

El script triggea el DAG `yelp_pipeline` y muestra el estado de cada tarea en tiempo real. Al terminar imprime las URLs del UI de Airflow y del dashboard.

### 4. Airflow UI

```
http://localhost:8080   →   admin / admin
```

---

## KPIs implementados

Todos calculados por fecha y persistidos en Cassandra (`kpi_results`). El dashboard los muestra con delta respecto al día anterior.

| # | KPI | Descripción | Fuente |
|---|---|---|---|
| 1 | `reviews_per_day` | Reseñas procesadas ese día | `daily_review_counts` |
| 2 | `daily_growth_pct` | Crecimiento % vs día anterior | `daily_review_counts` |
| 3 | `top_category_avg_stars` | Rating de la categoría mejor calificada (mín 3 reseñas) | `category_daily_stats` |
| 4 | `active_categories` | Nº de categorías con al menos 1 reseña | `category_daily_stats` |
| 5 | `avg_stars_of_day` | Rating promedio ponderado por volumen | `category_daily_stats` |
| 6 | `avg_sentiment` | Sentimiento VADER promedio (–1 → +1) | `category_daily_stats` |
| 7 | `top_category_quality_score` | Score `stars × (sentiment+1) × reviews` | `category_daily_stats` |
| 8 | `pct_high_rated_categories` | % categorías con avg_stars ≥ 4.0 | `category_daily_stats` |
| 9 | `pct_positive_categories` | % categorías con sentimiento VADER > 0.1 | `category_daily_stats` |

Adicionalmente, el dashboard muestra **4 métricas estructurales de Neo4j** (usuario más influyente, negocio más reseñado, total FRIEND / REVIEWED) calculadas una sola vez sobre el grafo completo.

---

## Equipo

| Integrante | Código | Rol |
|---|---|---|
| Quenta Solis, Jorge Eduardo | 202310623 | Data Engineer · Airflow · Dashboard |
| Velo Poma, Juan David | 202410587 | MongoDB |
| Cortez Rojas, Melanie Alexia | 202210100 | Cassandra |
| Maguiña Aranda, Paola Mercedes | 202120695 | Neo4j |
| Maquera Bobadilla, Diva Stewart | 202310599 | ETL · Limpieza |
| Yangali Cáceres, Luciana | 202310298 | KPIs · Informe |

**Docente:** Lezama Benavides, Aldo Martin

---

## Repositorio

[https://github.com/jorge-qs/Proyecto-Final-Big-Data](https://github.com/jorge-qs/Proyecto-Final-Big-Data)
