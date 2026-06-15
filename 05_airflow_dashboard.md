# Rol 5 — Airflow + Dashboard + DevOps (integración)

> Tú no reimplementas las bases: **cableas** lo que los otros cuatro ya escribieron.
> Eres el pegamento del proyecto y el dueño del entorno común.

---

## Objetivo
1. Levantar el entorno compartido (`docker-compose`) para que los 5 trabajen igual.
2. Orquestar el pipeline en un DAG de Airflow importando los loaders de los demás.
3. Construir el dashboard (Streamlit + Plotly) que lee de las 3 bases.
4. Calcular los KPIs (`src/kpis/`).

## Tu alcance
- `docker-compose.yml`, `.env.example`, `requirements.txt`
- `dags/yelp_pipeline_dag.py`
- `src/kpis/compute_kpis.py`
- `dashboard/app.py`

---

## A) Entorno compartido (semana 1 — desbloquea a todos)
Un `docker-compose.yml` con: `mongo`, `cassandra`, `neo4j`, `airflow` (webserver +
scheduler) y `postgres` (metadata de Airflow). Expón puertos estándar (27017, 9042,
7687/7474, 8080) y monta el repo en los contenedores. Entrega un `.env.example` con
`MONGO_URI`, `CASSANDRA_HOSTS`, `NEO4J_URI`, etc. Meta: `docker compose up` y las 5
máquinas tienen el mismo sistema corriendo.

## B) El DAG (la respuesta a "cómo junto todo")
**No reescribes ingestas.** Importas las funciones de tus compañeros:

```python
# dags/yelp_pipeline_dag.py
from src.transform import clean
from src.loaders import mongo_loader, cassandra_loader, neo4j_loader
from src.kpis import compute_kpis

# extract >> validate_clean >> load_mongo >> [cassandra, neo4j] >> generate_kpis
extract       = PythonOperator(task_id="extract", ...)
validate      = PythonOperator(task_id="validate_clean",
                               python_callable=clean.build_staging_for_date,
                               op_kwargs={"ds": "{{ ds }}"})
load_mongo    = PythonOperator(task_id="load_mongo",
                               python_callable=mongo_loader.load_reviews, ...)
to_cassandra  = PythonOperator(task_id="transform_load_cassandra",
                               python_callable=cassandra_loader.upsert_daily_counts, ...)
to_neo4j      = PythonOperator(task_id="build_load_neo4j",
                               python_callable=neo4j_loader.build_friend_edges, ...)
gen_kpis      = PythonOperator(task_id="generate_kpis",
                               python_callable=compute_kpis.run, op_kwargs={"ds":"{{ ds }}"})

extract >> validate >> load_mongo >> [to_cassandra, to_neo4j] >> gen_kpis
```
- `schedule="@daily"`, `catchup=True` para "rellenar" varios días y simular el
  incremental. Cada corrida procesa el slice `{{ ds }}`.
- Define la **interfaz de cada función con los dueños** (qué recibe, qué devuelve).
  Eso es lo único que negocias; el cuerpo lo escriben ellos.

## C) KPIs (`src/kpis/compute_kpis.py`)
Calcula y escribe los resultados en la tabla `kpi_results` de Cassandra (acuérdalo
con el de Cassandra). Cada KPI debe tener en el informe **definición, fórmula e
interpretación**:

| KPI | Definición | Fórmula | Interpretación | Fuente |
|---|---|---|---|---|
| 1. Reseñas/día | Volumen procesado por corrida | `COUNT(reviews date=D)` | Salud y carga del pipeline | Cassandra |
| 2. Crecimiento diario % | Variación día a día | `(C_D − C_{D-1})/C_{D-1}×100` | ¿La actividad sube o baja? | Cassandra |
| 3. Rating prom. por categoría | Calidad por segmento | `AVG(stars) GROUP BY category` | Qué rubros gustan más | Mongo/Cassandra |
| 4. Usuario más influyente | Nodo de mayor grado/PageRank | `max grado FRIEND` | Detectar influencers | Neo4j |
| 5. Top negocios de la red | Negocios más reseñados por amigos | `count reseñas de amigos` | Recomendación social | Neo4j+Mongo |
| 6. Sentimiento promedio | Tono medio de reseñas | `AVG(sentiment)` | ¿Clientes contentos? | enriquecimiento |

## D) Dashboard (`dashboard/app.py`, Streamlit + Plotly)
- **3 pestañas o secciones:** Documentos (Mongo), Tendencias (Cassandra), Red (Neo4j).
- Tarjetas de KPI con `st.metric`, filtros por ciudad/categoría/fecha (la parte de
  "jugar con la data"), gráficos Plotly (línea temporal, barras por categoría, grafo
  de red con `pyvis`/`plotly`).
- Buenas prácticas: cachea conexiones (`st.cache_resource`) y consultas
  (`st.cache_data`), tema en `.streamlit/config.toml`, no consultes en cada rerun.

---

## Qué documentar
- Diagrama del DAG (capturado de la UI de Airflow) y explicación de cada tarea.
- `docker-compose` y cómo se levanta el entorno.
- Las 6 fórmulas de KPI con interpretación.
- Capturas del dashboard (van al informe).

## Definición de "terminado"
- [ ] `docker compose up` levanta todo en cualquier máquina.
- [ ] DAG corre de punta a punta y es programado/idempotente.
- [ ] `kpi_results` poblada por la etapa final.
- [ ] Dashboard lee de las 3 bases y muestra los KPIs con filtros.

## Prompt sugerido para tu agente
> "Soy el responsable de orquestación y dashboard en un proyecto Big Data sobre Yelp
> con MongoDB, Cassandra y Neo4j. Ayúdame a: (1) un `docker-compose.yml` con mongo,
> cassandra, neo4j, postgres y Airflow (webserver+scheduler), con `.env.example`; (2)
> `dags/yelp_pipeline_dag.py` que importe funciones de `src/transform`,
> `src/loaders/*` y `src/kpis` con el flujo extract → validate → load_mongo →
> [cassandra, neo4j] → generate_kpis, schedule @daily y catchup; (3)
> `src/kpis/compute_kpis.py` que calcule 6 KPIs y los escriba en la tabla
> kpi_results de Cassandra; (4) `dashboard/app.py` en Streamlit + Plotly con 3
> secciones (Mongo/Cassandra/Neo4j), tarjetas st.metric, filtros y caching. No
> reimplementes la lógica de carga: solo importa y llama las funciones existentes."
