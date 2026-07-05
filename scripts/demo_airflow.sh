#!/bin/bash
# ──────────────────────────────────────────────────────────────────────────────
# Demo de Airflow para el video del proyecto
# Triggea el DAG yelp_pipeline para la fecha 2019-01-27 (97 reseñas reales)
# y monitorea la ejecución en tiempo real.
#
# Uso:
#   bash scripts/demo_airflow.sh
#   bash scripts/demo_airflow.sh --date 2019-01-28    # otra fecha del dataset
# ──────────────────────────────────────────────────────────────────────────────
set -euo pipefail

GREEN='\033[0;32m'; YELLOW='\033[1;33m'; RED='\033[0;31m'; CYAN='\033[0;36m'
BOLD='\033[1m'; NC='\033[0m'

AIRFLOW_URL="http://localhost:8080"
AIRFLOW_USER="admin"
AIRFLOW_PASS="admin"
DAG_ID="yelp_pipeline"
DEMO_DATE="${2:-2019-01-27}"   # segunda arg o default

# Acepta --date YYYY-MM-DD como opción
while [[ $# -gt 0 ]]; do
    case $1 in
        --date) DEMO_DATE="$2"; shift 2 ;;
        *) shift ;;
    esac
done

log()  { echo -e "${GREEN}[✓]${NC} $1"; }
warn() { echo -e "${YELLOW}[~]${NC} $1"; }
err()  { echo -e "${RED}[✗]${NC} $1"; exit 1; }
info() { echo -e "${CYAN}[i]${NC} $1"; }

echo ""
echo -e "${BOLD}═══════════════════════════════════════════════════════${NC}"
echo -e "${BOLD}   Yelp Big Data — Demo Airflow para video              ${NC}"
echo -e "${BOLD}═══════════════════════════════════════════════════════${NC}"
echo ""
info "DAG       : $DAG_ID"
info "Fecha demo: $DEMO_DATE  (97 reseñas de Philadelphia, PA)"
echo ""

# ── 1. Verificar que Airflow esté corriendo ────────────────────────────────
warn "Verificando Airflow en $AIRFLOW_URL ..."
HTTP_CODE=$(curl -s -o /dev/null -w "%{http_code}" \
    -u "$AIRFLOW_USER:$AIRFLOW_PASS" \
    "$AIRFLOW_URL/api/v1/health" 2>/dev/null || echo "000")

if [[ "$HTTP_CODE" != "200" ]]; then
    err "Airflow no responde (HTTP $HTTP_CODE). Ejecuta primero: bash start.sh"
fi
log "Airflow disponible"

# ── 2. Verificar que el DAG existe y está activo ───────────────────────────
DAG_INFO=$(curl -s -u "$AIRFLOW_USER:$AIRFLOW_PASS" \
    "$AIRFLOW_URL/api/v1/dags/$DAG_ID" 2>/dev/null)
IS_PAUSED=$(echo "$DAG_INFO" | python3 -c "import sys,json; d=json.load(sys.stdin); print(d.get('is_paused','unknown'))" 2>/dev/null || echo "unknown")

if [[ "$IS_PAUSED" == "True" ]]; then
    warn "DAG está pausado. Activando..."
    curl -s -X PATCH -u "$AIRFLOW_USER:$AIRFLOW_PASS" \
        -H "Content-Type: application/json" \
        -d '{"is_paused": false}' \
        "$AIRFLOW_URL/api/v1/dags/$DAG_ID" > /dev/null
    log "DAG activado"
fi

# ── 3. Triggear el DAG para la fecha de demo ──────────────────────────────
LOGICAL_DATE="${DEMO_DATE}T00:00:00+00:00"

# Si ya existe un run para esa fecha, eliminarlo primero
EXISTING_RUN_ID=$(curl -s -u "$AIRFLOW_USER:$AIRFLOW_PASS" \
    "$AIRFLOW_URL/api/v1/dags/$DAG_ID/dagRuns?limit=100&order_by=-execution_date" 2>/dev/null | \
    python3 -c "
import sys,json
d=json.load(sys.stdin)
runs=[r for r in d.get('dag_runs',[]) if r.get('logical_date','').startswith('$DEMO_DATE')]
print(runs[0]['dag_run_id'] if runs else '')
" 2>/dev/null || echo "")

if [[ -n "$EXISTING_RUN_ID" ]]; then
    warn "Existe un run previo para $DEMO_DATE ($EXISTING_RUN_ID). Eliminándolo para el demo..."
    curl -s -X DELETE \
        -u "$AIRFLOW_USER:$AIRFLOW_PASS" \
        "$AIRFLOW_URL/api/v1/dags/$DAG_ID/dagRuns/$EXISTING_RUN_ID" > /dev/null
    log "Run anterior eliminado"
    sleep 2
fi

RUN_ID="manual_demo_${DEMO_DATE}_$(date +%s)"
warn "Triggeando DAG para logical_date=$LOGICAL_DATE ..."

TRIGGER_RESP=$(curl -s -X POST \
    -u "$AIRFLOW_USER:$AIRFLOW_PASS" \
    -H "Content-Type: application/json" \
    -d "{\"dag_run_id\": \"$RUN_ID\", \"logical_date\": \"$LOGICAL_DATE\"}" \
    "$AIRFLOW_URL/api/v1/dags/$DAG_ID/dagRuns" 2>/dev/null)

ACTUAL_RUN_ID=$(echo "$TRIGGER_RESP" | python3 -c \
    "import sys,json; d=json.load(sys.stdin); print(d.get('dag_run_id',''))" 2>/dev/null || echo "")

if [[ -n "$ACTUAL_RUN_ID" ]]; then
    RUN_ID="$ACTUAL_RUN_ID"
else
    err "No se pudo crear el run. Respuesta: $(echo $TRIGGER_RESP | head -c 300). Revisa Airflow en $AIRFLOW_URL"
fi
log "DAG run creado: $RUN_ID"
echo ""
info "Puedes seguir el progreso en:"
info "  $AIRFLOW_URL/dags/$DAG_ID/grid"
echo ""

# ── 4. Polling de estado ──────────────────────────────────────────────────
TASKS=("extract" "validate_clean" "load_mongo" "transform_load_cassandra" "build_load_neo4j" "generate_kpis")
POLL_INTERVAL=6
MAX_WAIT=600   # 10 minutos máximo
elapsed=0
last_state=""

echo -e "${BOLD}Monitoreando ejecución (Ctrl+C para salir sin cancelar el run)...${NC}"
echo ""

while [[ $elapsed -lt $MAX_WAIT ]]; do
    sleep $POLL_INTERVAL
    elapsed=$((elapsed + POLL_INTERVAL))

    # Estado del DAG run
    RUN_STATE=$(curl -s -u "$AIRFLOW_USER:$AIRFLOW_PASS" \
        "$AIRFLOW_URL/api/v1/dags/$DAG_ID/dagRuns/$RUN_ID" 2>/dev/null | \
        python3 -c "import sys,json; d=json.load(sys.stdin); print(d.get('state','?'))" 2>/dev/null || echo "?")

    # Estado de cada task
    TASK_STATES=$(curl -s -u "$AIRFLOW_USER:$AIRFLOW_PASS" \
        "$AIRFLOW_URL/api/v1/dags/$DAG_ID/dagRuns/$RUN_ID/taskInstances" 2>/dev/null | \
        python3 -c "
import sys,json
d=json.load(sys.stdin)
tasks={t['task_id']:t.get('state','?') for t in d.get('task_instances',[])}
for tid,st in tasks.items():
    icon='✅' if st=='success' else ('🔄' if st=='running' else ('❌' if st=='failed' else '⏳'))
    print(f'  {icon} {tid}: {st}')
" 2>/dev/null || echo "  (obteniendo estado...)")

    # Limpiar pantalla y mostrar estado
    printf "\r\033[K"
    echo -e "${CYAN}[${elapsed}s]${NC} Estado del run: ${BOLD}$RUN_STATE${NC}"
    echo "$TASK_STATES"
    echo ""

    if [[ "$RUN_STATE" == "success" ]]; then
        echo -e "${GREEN}${BOLD}═══════════════════════════════════════${NC}"
        echo -e "${GREEN}${BOLD}   ✓ Pipeline completado exitosamente   ${NC}"
        echo -e "${GREEN}${BOLD}═══════════════════════════════════════${NC}"
        echo ""
        log "Fecha procesada: $DEMO_DATE"
        log "Airflow UI  → $AIRFLOW_URL/dags/$DAG_ID/grid"
        log "Dashboard   → http://localhost:8501"
        echo ""
        break
    elif [[ "$RUN_STATE" == "failed" ]]; then
        echo -e "${RED}${BOLD}Pipeline falló.${NC}"
        err "Revisa los logs en $AIRFLOW_URL/dags/$DAG_ID/grid"
    fi

    # Mostrar spinner mientras espera
    printf "${YELLOW}Esperando siguiente check en ${POLL_INTERVAL}s...${NC}"
done

if [[ $elapsed -ge $MAX_WAIT && "$RUN_STATE" != "success" ]]; then
    warn "Tiempo máximo de espera alcanzado (${MAX_WAIT}s)."
    warn "El pipeline sigue corriendo. Revisa en: $AIRFLOW_URL/dags/$DAG_ID/grid"
fi
