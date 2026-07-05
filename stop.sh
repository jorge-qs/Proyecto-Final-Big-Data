#!/bin/bash
# Baja todo el proyecto limpiamente
set -e

PROJECT_DIR="$(cd "$(dirname "$0")" && pwd)"
cd "$PROJECT_DIR"

GREEN='\033[0;32m'; YELLOW='\033[1;33m'; NC='\033[0m'

log()  { echo -e "${GREEN}[✓]${NC} $1"; }
warn() { echo -e "${YELLOW}[~]${NC} $1"; }

echo ""
echo "================================================"
echo "   Yelp Big Data — Apagando el entorno"
echo "================================================"

# 1. Detener dashboard
warn "Deteniendo dashboard Streamlit..."
pkill -f "streamlit run dashboard/app.py" 2>/dev/null && log "Dashboard detenido" || log "Dashboard no estaba corriendo"

# 2. Bajar contenedores (mantiene los volúmenes — los datos se conservan)
warn "Bajando contenedores Docker..."
docker compose down
log "Contenedores detenidos"

echo ""
echo "================================================"
echo -e "${GREEN}  Entorno apagado.${NC} Los datos están guardados en"
echo "  los volúmenes Docker y en data/staging/."
echo "  Para volver a levantar:  ./start.sh"
echo "================================================"
echo ""
