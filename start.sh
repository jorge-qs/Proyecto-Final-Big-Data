#!/bin/bash
# Levanta todo el proyecto: BDs + Airflow + Dashboard
set -e

PROJECT_DIR="$(cd "$(dirname "$0")" && pwd)"
cd "$PROJECT_DIR"

GREEN='\033[0;32m'; YELLOW='\033[1;33m'; RED='\033[0;31m'; NC='\033[0m'

log()  { echo -e "${GREEN}[✓]${NC} $1"; }
warn() { echo -e "${YELLOW}[~]${NC} $1"; }
err()  { echo -e "${RED}[✗]${NC} $1"; exit 1; }

echo ""
echo "================================================"
echo "   Yelp Big Data — Inicio del entorno"
echo "================================================"

# 1. Verificar Docker
docker info > /dev/null 2>&1 || err "Docker no está corriendo. Abre Docker Desktop primero."
log "Docker disponible"

# 2. Levantar contenedores
warn "Levantando contenedores (MongoDB, Cassandra, Neo4j, Airflow)..."
docker compose up -d mongo neo4j postgres airflow-webserver airflow-scheduler cassandra

# 3. Esperar Cassandra (es el más lento)
warn "Esperando que Cassandra esté lista..."
for i in $(seq 1 30); do
    STATUS=$(docker inspect --format='{{.State.Health.Status}}' bigdataproyecto2-cassandra-1 2>/dev/null)
    [ "$STATUS" = "healthy" ] && break
    [ $i -eq 30 ] && err "Cassandra tardó demasiado. Revisa con: docker compose logs cassandra"
    sleep 5
done
log "Cassandra lista"

# 4. Aplicar schema de Cassandra (se pierde al reiniciar el contenedor)
warn "Aplicando schema de Cassandra..."
docker exec -i bigdataproyecto2-cassandra-1 cqlsh < scripts/cassandra_schema.cql 2>/dev/null
log "Schema aplicado"

# 5. Esperar Airflow webserver
warn "Esperando Airflow UI..."
for i in $(seq 1 20); do
    CODE=$(curl -s -o /dev/null -w "%{http_code}" http://localhost:8080/health 2>/dev/null)
    [ "$CODE" = "200" ] && break
    sleep 5
done
log "Airflow UI lista"

# 6. Activar DAG
docker compose exec -T airflow-scheduler airflow dags unpause yelp_pipeline > /dev/null 2>&1 || true
log "DAG yelp_pipeline activo"

# 7. Levantar dashboard en background
warn "Iniciando dashboard Streamlit..."
pkill -f "streamlit run dashboard/app.py" 2>/dev/null || true
PYTHONPATH="$PROJECT_DIR" \
    "$PROJECT_DIR/.venv/bin/streamlit" run dashboard/app.py \
    --server.port 8501 --server.headless true \
    > /tmp/streamlit.log 2>&1 &

sleep 4
CODE=$(curl -s -o /dev/null -w "%{http_code}" http://localhost:8501 2>/dev/null)
[ "$CODE" = "200" ] && log "Dashboard listo" || warn "Dashboard tardando, revisa /tmp/streamlit.log"

echo ""
echo "================================================"
echo -e "${GREEN}  Todo listo!${NC}"
echo "================================================"
echo "  Airflow UI  →  http://localhost:8080  (admin / admin)"
echo "  Dashboard   →  http://localhost:8501"
echo "  Neo4j       →  http://localhost:7474  (neo4j / neo4j_password)"
echo ""
echo "  Para bajar todo:  ./stop.sh"
echo "================================================"
echo ""
