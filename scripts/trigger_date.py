"""
Trigger manual del DAG yelp_pipeline para una fecha específica.
Útil para el video: dispara el pipeline y espera que termine.

Uso:
    PYTHONPATH=. .venv/bin/python3 scripts/trigger_date.py 2019-01-28
    PYTHONPATH=. .venv/bin/python3 scripts/trigger_date.py 2019-01-29
    PYTHONPATH=. .venv/bin/python3 scripts/trigger_date.py --list   # ver fechas disponibles
"""
from __future__ import annotations
import sys
import time
import json
import urllib.request
import urllib.error
import base64

AIRFLOW_URL  = "http://localhost:8080"
AIRFLOW_USER = "admin"
AIRFLOW_PASS = "admin"
DAG_ID       = "yelp_pipeline"

_AUTH = base64.b64encode(f"{AIRFLOW_USER}:{AIRFLOW_PASS}".encode()).decode()


def _req(method: str, path: str, body: dict | None = None):
    url = f"{AIRFLOW_URL}/api/v1{path}"
    data = json.dumps(body).encode() if body else None
    req = urllib.request.Request(
        url, data=data, method=method,
        headers={
            "Authorization": f"Basic {_AUTH}",
            "Content-Type": "application/json",
        },
    )
    try:
        with urllib.request.urlopen(req, timeout=10) as resp:
            return json.loads(resp.read())
    except urllib.error.HTTPError as e:
        return json.loads(e.read())
    except Exception as e:
        print(f"  Error de conexión: {e}")
        return {}


def list_available_dates():
    """Muestra fechas que tienen datos en Cassandra pero no tienen KPIs (candidatas para demo)."""
    try:
        from src.common.config import CASSANDRA_HOSTS, CASSANDRA_PORT, CASSANDRA_KEYSPACE
        from cassandra.cluster import Cluster
        cluster = Cluster(CASSANDRA_HOSTS, port=CASSANDRA_PORT)
        session = cluster.connect(CASSANDRA_KEYSPACE)

        all_dates  = {str(r.review_date) for r in session.execute("SELECT review_date FROM daily_review_counts")}
        kpi_dates  = {str(r.kpi_date)    for r in session.execute("SELECT kpi_date FROM kpi_results")}
        cluster.shutdown()

        demo_dates = sorted(all_dates - kpi_dates)
        loaded     = sorted(all_dates & kpi_dates)

        print("\nFechas YA procesadas con KPIs:")
        for d in loaded[-5:]:
            print(f"  ✅  {d}")
        if len(loaded) > 5:
            print(f"  ... y {len(loaded) - 5} más")

        print("\nFechas disponibles para demo (con data pero sin KPIs aún):")
        if demo_dates:
            for d in demo_dates:
                print(f"  🎯  {d}  ← usa esta con trigger_date.py")
        else:
            print("  (ninguna — todas las fechas ya tienen KPIs)")
            print("  Puedes re-triggear cualquier fecha; el DAG es idempotente.")
            for d in loaded[-3:]:
                print(f"       {d}")

    except Exception as e:
        print(f"Error conectando a Cassandra: {e}")
        print("Asegúrate de que los contenedores estén corriendo (bash start.sh)")


def trigger(date_str: str):
    logical_date = f"{date_str}T00:00:00+00:00"
    print(f"\n{'='*50}")
    print(f"  Triggering DAG: {DAG_ID}")
    print(f"  Fecha:          {date_str}")
    print(f"{'='*50}\n")

    # Verificar Airflow
    health = _req("GET", "/health")
    if not health:
        print("❌  Airflow no responde. Ejecuta: bash start.sh")
        sys.exit(1)

    # Activar DAG si está pausado
    dag_info = _req("GET", f"/dags/{DAG_ID}")
    if dag_info.get("is_paused"):
        print("  DAG pausado — activando...")
        _req("PATCH", f"/dags/{DAG_ID}", {"is_paused": False})

    # Triggear
    resp = _req("POST", f"/dags/{DAG_ID}/dagRuns", {"logical_date": logical_date})
    run_id = resp.get("dag_run_id", "")

    if not run_id:
        # Puede ser que ya exista un run para esa fecha
        detail = resp.get("detail", "")
        if "already exists" in detail or "already" in detail.lower():
            print(f"  ⚠  Ya existe un run para {date_str}. Buscando run activo...")
            runs = _req("GET", f"/dags/{DAG_ID}/dagRuns?limit=10&order_by=-execution_date")
            for r in runs.get("dag_runs", []):
                if date_str in r.get("logical_date", ""):
                    run_id = r["dag_run_id"]
                    break
        if not run_id:
            print(f"  ❌  No se pudo crear el run. Respuesta: {resp}")
            sys.exit(1)

    print(f"  Run ID: {run_id}")
    print(f"  Sigue el progreso en:")
    print(f"  {AIRFLOW_URL}/dags/{DAG_ID}/grid\n")

    # Polling
    TASKS = [
        ("extract",                  "Extrayendo archivos raw"),
        ("validate_clean",           "Limpiando y generando staging"),
        ("load_mongo",               "Cargando MongoDB"),
        ("transform_load_cassandra", "Cargando Cassandra (series temporales)"),
        ("build_load_neo4j",         "Cargando Neo4j (grafo)"),
        ("generate_kpis",            "Calculando KPIs"),
    ]
    seen_states: dict[str, str] = {}
    start = time.time()

    while True:
        time.sleep(5)
        elapsed = int(time.time() - start)

        run_info = _req("GET", f"/dags/{DAG_ID}/dagRuns/{run_id}")
        state    = run_info.get("state", "?")

        task_resp = _req("GET", f"/dags/{DAG_ID}/dagRuns/{run_id}/taskInstances")
        task_map  = {t["task_id"]: t.get("state", "?") for t in task_resp.get("task_instances", [])}

        # Imprimir cambios de estado
        for tid, desc in TASKS:
            tstate = task_map.get(tid, "pending")
            if tstate != seen_states.get(tid):
                seen_states[tid] = tstate
                icon = {"success": "✅", "running": "🔄", "failed": "❌",
                        "upstream_failed": "⛔"}.get(tstate, "⏳")
                print(f"  [{elapsed:>3}s] {icon} {desc} ({tstate})")

        if state == "success":
            print(f"\n  ✅  Pipeline completado en {elapsed}s\n")
            print(f"  Dashboard → http://localhost:8501")
            print(f"  Airflow   → {AIRFLOW_URL}/dags/{DAG_ID}/grid")
            break
        elif state == "failed":
            print(f"\n  ❌  Pipeline falló en {elapsed}s")
            print(f"  Ver logs → {AIRFLOW_URL}/dags/{DAG_ID}/grid")
            sys.exit(1)
        elif elapsed > 600:
            print(f"\n  ⏱  Timeout (10 min). El run sigue en {AIRFLOW_URL}/dags/{DAG_ID}/grid")
            break


def main():
    if len(sys.argv) < 2 or sys.argv[1] in ("-h", "--help"):
        print(__doc__)
        sys.exit(0)

    if sys.argv[1] == "--list":
        list_available_dates()
        return

    date_str = sys.argv[1]
    # Validar formato básico
    if len(date_str) != 10 or date_str[4] != "-" or date_str[7] != "-":
        print(f"Formato inválido: '{date_str}'. Usa YYYY-MM-DD (ej. 2019-01-28)")
        sys.exit(1)

    trigger(date_str)


if __name__ == "__main__":
    main()
