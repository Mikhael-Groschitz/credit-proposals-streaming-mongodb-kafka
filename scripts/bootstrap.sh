#!/usr/bin/env bash
set -euo pipefail

RAIZ_PROJETO="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
COMPOSE_FILE="${RAIZ_PROJETO}/infra/docker-compose.yml"
ENV_FILE="${RAIZ_PROJETO}/infra/.env"
ENV_EXAMPLE="${RAIZ_PROJETO}/infra/.env.example"

FORCAR_BUILD=0
if [[ "${1:-}" == "--build" ]]; then
    FORCAR_BUILD=1
fi

log() { echo "[bootstrap] $*"; }
erro() { echo "[bootstrap] ERRO: $*" >&2; }

if ! command -v docker >/dev/null 2>&1; then
    erro "docker nao encontrado no PATH. Instale o Docker Desktop com integracao WSL2 antes de continuar."
    exit 1
fi
if ! docker compose version >/dev/null 2>&1; then
    erro "plugin 'docker compose' (v2) nao encontrado. Atualize o Docker Desktop."
    exit 1
fi

if [[ ! -f "${ENV_FILE}" ]]; then
    log "infra/.env nao existe, copiando de infra/.env.example..."
    cp "${ENV_EXAMPLE}" "${ENV_FILE}"
fi

set -a
source "${ENV_FILE}"
set +a

if [[ -z "${KAFKA_CLUSTER_ID:-}" ]]; then
    log "KAFKA_CLUSTER_ID vazio, gerando um novo (unico por ambiente, sera reaproveitado nas proximas subidas)..."
    NOVO_CLUSTER_ID="$("${RAIZ_PROJETO}/scripts/generate-kafka-cluster-id.sh")"
    if grep -q '^KAFKA_CLUSTER_ID=' "${ENV_FILE}"; then
        sed -i.bak "s|^KAFKA_CLUSTER_ID=.*|KAFKA_CLUSTER_ID=${NOVO_CLUSTER_ID}|" "${ENV_FILE}"
        rm -f "${ENV_FILE}.bak"
    else
        echo "KAFKA_CLUSTER_ID=${NOVO_CLUSTER_ID}" >> "${ENV_FILE}"
    fi
    export KAFKA_CLUSTER_ID="${NOVO_CLUSTER_ID}"
    log "KAFKA_CLUSTER_ID=${NOVO_CLUSTER_ID} salvo em infra/.env"
else
    log "KAFKA_CLUSTER_ID ja definido em infra/.env, reaproveitando."
fi

COMPOSE=(docker compose -f "${COMPOSE_FILE}" --env-file "${ENV_FILE}")

if [[ "${FORCAR_BUILD}" -eq 1 ]]; then
    log "reconstruindo imagens custom (--build explicito)..."
    "${COMPOSE[@]}" build
fi

log "subindo servicos..."
"${COMPOSE[@]}" up -d

SERVICOS_COM_HEALTHCHECK=(mongo mongo-init kafka kafka-init minio minio-init spark-master spark-worker)
SERVICOS_SO_RUNNING=(generator)
MAX_TENTATIVAS=90
INTERVALO_SEGUNDOS=2

esperar_saudavel() {
    local servico="$1" tentativa=0 container_id status
    while true; do
        container_id="$("${COMPOSE[@]}" ps -q "${servico}")"
        if [[ -z "${container_id}" ]]; then
            status="ausente"
        else
            status="$(docker inspect --format='{{.State.Health.Status}}' "${container_id}" 2>/dev/null || echo "sem-healthcheck")"
        fi
        if [[ "${status}" == "healthy" ]]; then
            log "servico '${servico}' saudavel."
            return 0
        fi
        tentativa=$((tentativa + 1))
        if [[ "${tentativa}" -ge "${MAX_TENTATIVAS}" ]]; then
            erro "servico '${servico}' nao ficou saudavel a tempo (ultimo status: ${status})."
            erro "veja os logs com: docker compose -f infra/docker-compose.yml --env-file infra/.env logs ${servico}"
            return 1
        fi
        sleep "${INTERVALO_SEGUNDOS}"
    done
}

esperar_rodando() {
    local servico="$1" tentativa=0 container_id status
    while true; do
        container_id="$("${COMPOSE[@]}" ps -q "${servico}")"
        if [[ -n "${container_id}" ]]; then
            status="$(docker inspect --format='{{.State.Status}}' "${container_id}" 2>/dev/null || echo "ausente")"
            if [[ "${status}" == "running" ]]; then
                log "servico '${servico}' em execucao."
                return 0
            fi
        fi
        tentativa=$((tentativa + 1))
        if [[ "${tentativa}" -ge "${MAX_TENTATIVAS}" ]]; then
            erro "servico '${servico}' nao ficou em execucao a tempo."
            erro "veja os logs com: docker compose -f infra/docker-compose.yml --env-file infra/.env logs ${servico}"
            return 1
        fi
        sleep "${INTERVALO_SEGUNDOS}"
    done
}

FALHOU=0
for servico in "${SERVICOS_COM_HEALTHCHECK[@]}"; do
    esperar_saudavel "${servico}" || FALHOU=1
done
for servico in "${SERVICOS_SO_RUNNING[@]}"; do
    esperar_rodando "${servico}" || FALHOU=1
done

if [[ "${FALHOU}" -eq 1 ]]; then
    erro "um ou mais servicos falharam ao iniciar. Ambiente NAO esta pronto."
    exit 1
fi

log ""
log "ambiente pronto:"
log "  MongoDB:        mongodb://localhost:27017/?replicaSet=${MONGO_REPLICA_SET:-rs0}"
log "  Kafka:          localhost:9092 (topico: ${KAFKA_TOPIC_PROPOSTAS:-propostas.cdc})"
log "  MinIO console:  http://localhost:9001 (usuario: ${MINIO_ROOT_USER:-minioadmin})"
log "  Spark UI:       http://localhost:8080"
log ""
log "acompanhe o gerador de carga com:"
log "  docker compose -f infra/docker-compose.yml --env-file infra/.env logs -f generator"
