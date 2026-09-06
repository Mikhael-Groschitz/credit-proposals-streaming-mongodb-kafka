#!/usr/bin/env bash
set -euo pipefail

RAIZ_PROJETO="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
COMPOSE_FILE="${RAIZ_PROJETO}/infra/docker-compose.yml"
ENV_FILE="${RAIZ_PROJETO}/infra/.env"

MANTER_CLUSTER_ID=0
LIMPAR_IMAGENS=0
for arg in "$@"; do
    case "${arg}" in
        --keep-cluster-id) MANTER_CLUSTER_ID=1 ;;
        --prune-images) LIMPAR_IMAGENS=1 ;;
        *)
            echo "[teardown] argumento desconhecido: ${arg}" >&2
            echo "uso: scripts/teardown.sh [--keep-cluster-id] [--prune-images]" >&2
            exit 1
            ;;
    esac
done

log() { echo "[teardown] $*"; }

if [[ ! -f "${COMPOSE_FILE}" ]]; then
    log "docker-compose.yml nao encontrado em ${COMPOSE_FILE}, nada a derrubar."
    exit 0
fi

log "derrubando servicos e removendo volumes nomeados (mongo-data, kafka-data, minio-data)..."
docker compose -f "${COMPOSE_FILE}" --env-file "${ENV_FILE}" down -v --remove-orphans

if [[ -f "${ENV_FILE}" ]]; then
    if [[ "${MANTER_CLUSTER_ID}" -eq 1 ]]; then
        log "--keep-cluster-id passado: mantendo KAFKA_CLUSTER_ID em infra/.env."
    else
        log "removendo KAFKA_CLUSTER_ID de infra/.env (o volume do Kafka foi removido, um novo sera gerado na proxima subida)..."
        sed -i.bak 's|^KAFKA_CLUSTER_ID=.*|KAFKA_CLUSTER_ID=|' "${ENV_FILE}"
        rm -f "${ENV_FILE}.bak"
    fi
fi

if [[ "${LIMPAR_IMAGENS}" -eq 1 ]]; then
    log "removendo imagens custom (credpipe/spark, credpipe/generator)..."
    docker image rm -f credpipe/spark:3.5.8 credpipe/generator:1.0 2>/dev/null || true
fi

log "ambiente derrubado."
