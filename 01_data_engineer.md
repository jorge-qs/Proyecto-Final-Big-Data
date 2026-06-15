# Rol 1 — Data Engineer (datos, extracción y limpieza)

> Eres la base de todo el proyecto. Tú produces el **dato limpio canónico** que los
> otros tres consumen. Si tu contrato cambia, todos se rompen → congélalo temprano.

---

## Objetivo
Descargar el Yelp Open Dataset, explorarlo, validarlo, limpiarlo y dejarlo en
`data/staging/` con un **esquema documentado e inmutable**. Cubres las **etapas 1 y
2 del DAG** (Extracción + Validación/Limpieza).

## Tu alcance (lo que posees)
- `src/extract/` — lectura del raw de Yelp.
- `src/transform/` — validación, limpieza y enriquecimiento.
- `src/common/schema.py` — **el contrato de datos** (¡el activo más importante!).
- `data/staging/` — tu salida.

---

## Tareas concretas

1. **Descarga y EDA** (semana 1)
   - Baja los 5 JSON. Filtra a una región o categoría para quedar en ~50k–500k reseñas.
   - Analiza: nº de registros por archivo, rango de fechas, % de nulos, cardinalidad
     de `categories`, tamaño de la lista `friends`. Guarda gráficos en `docs/report_assets/`.

2. **Define el contrato** (`src/common/schema.py`) — congélalo y avisa al equipo.
   Ejemplo de la entidad reseña (haz lo mismo para business, user, checkin, tip):
   ```python
   from pydantic import BaseModel
   from datetime import date

   class Review(BaseModel):
       review_id: str
       user_id: str
       business_id: str
       stars: float          # 1–5
       useful: int
       funny: int
       cool: int
       text: str
       date: date            # normalizada a YYYY-MM-DD
       sentiment: float | None = None   # enriquecimiento (-1..1)
   ```

3. **Limpieza** (`src/transform/`)
   - Normaliza fechas a `YYYY-MM-DD`.
   - Quita duplicados por `review_id` / `business_id` / `user_id`.
   - `categories` (string "Mexican, Bars") → lista `["Mexican","Bars"]`.
   - `checkin.date` (lista separada por coma) → explota a una fila por timestamp.
   - Descarta registros sin claves obligatorias; loguea cuántos descartaste.

4. **Enriquecimiento** (lo que nos distingue)
   - Sentimiento del `text` de cada reseña (ej. `VADER`/`TextBlob`) → campo `sentiment`.

5. **Salida particionada por fecha** (habilita el incremental)
   ```
   data/staging/reviews/dt=YYYY-MM-DD/part.jsonl
   ```
   Función reutilizable que el DAG llamará por día lógico:
   ```python
   # src/transform/clean.py
   def build_staging_for_date(ds: str) -> dict:
       """Limpia y escribe el slice de la fecha ds. Devuelve conteos por entidad."""
   ```

---

## CRUD — tu parte
No tienes una BD propia, pero **diseñas las claves** que hacen posible el CRUD de los
demás (qué campo es la PK/_id en cada entidad). Documenta esa decisión.

## Qué documentar para el informe
- **Descripción del dataset** (origen, licencia educativa, estructura, nº registros).
- Decisiones de filtrado y por qué.
- Diagrama del flujo de limpieza (entrada → reglas → salida).
- Tabla "antes/después": registros crudos vs. registros válidos.
- El contrato de datos (`schema.py`) explicado.

## Definición de "terminado"
- [ ] `schema.py` congelado y comunicado al equipo.
- [ ] `build_staging_for_date(ds)` funciona y es idempotente (re-correr no duplica).
- [ ] `data/staging/` poblado con ≥ 50k reseñas.
- [ ] EDA + capturas en `docs/`.

## Prompt sugerido para tu agente
> "Soy el Data Engineer de un proyecto Big Data sobre el Yelp Open Dataset (5 JSON,
> un objeto por línea). Ayúdame a: (1) escribir `src/common/schema.py` con modelos
> Pydantic para business, review, user, checkin y tip según el contrato adjunto; (2)
> escribir `src/transform/clean.py` con una función idempotente
> `build_staging_for_date(ds)` que lea el raw, valide contra los modelos, normalice
> fechas, deduplique, explote los check-ins por timestamp, calcule sentimiento con
> VADER y escriba `.jsonl` particionado por fecha. Incluye logging de registros
> descartados y tests básicos en `tests/`."
