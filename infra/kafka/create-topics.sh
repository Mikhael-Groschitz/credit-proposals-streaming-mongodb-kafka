#!/usr/bin/env bash
set -euo pipefail

KAFKA_BOOTSTRAP_INTERNO="${KAFKA_BOOTSTRAP_INTERNO:-kafka:19092}"
KAFKA_TOPIC_PROPOSTAS="${KAFKA_TOPIC_PROPOSTAS:-propostas.cdc}"
KAFKA_TOPIC_PARTITIONS="${KAFKA_TOPIC_PARTITIONS:-6}"
KAFKA_TOPIC_REPLICATION_FACTOR="${KAFKA_TOPIC_REPLICATION_FACTOR:-1}"
KAFKA_BIN=/opt/kafka/bin
SENTINEL=/tmp/init-done
MAX_TENTATIVAS=60
INTERVALO_SEGUNDOS=2

log() { echo "[kafka-init] $*"; }

log "aguardando broker em ${KAFKA_BOOTSTRAP_INTERNO} responder..."
tentativa=0
until "${KAFKA_BIN}/kafka-broker-api-versions.sh" --bootstrap-server "${KAFKA_BOOTSTRAP_INTERNO}" >/dev/null 2>&1; do
    tentativa=$((tentativa + 1))
    if [ "${tentativa}" -ge "${MAX_TENTATIVAS}" ]; then
        log "ERRO: broker Kafka nao respondeu apos ${MAX_TENTATIVAS} tentativas."
        exit 1
    fi
    sleep "${INTERVALO_SEGUNDOS}"
done
log "broker respondendo."

log "criando topico '${KAFKA_TOPIC_PROPOSTAS}' (particoes=${KAFKA_TOPIC_PARTITIONS}, fator-replicacao=${KAFKA_TOPIC_REPLICATION_FACTOR}) se ainda nao existir..."
"${KAFKA_BIN}/kafka-topics.sh" \
    --bootstrap-server "${KAFKA_BOOTSTRAP_INTERNO}" \
    --create \
    --if-not-exists \
    --topic "${KAFKA_TOPIC_PROPOSTAS}" \
    --partitions "${KAFKA_TOPIC_PARTITIONS}" \
    --replication-factor "${KAFKA_TOPIC_REPLICATION_FACTOR}" \
    --config retention.ms=604800000 \
    --config cleanup.policy=delete

log "topico pronto:"
"${KAFKA_BIN}/kafka-topics.sh" --bootstrap-server "${KAFKA_BOOTSTRAP_INTERNO}" --describe --topic "${KAFKA_TOPIC_PROPOSTAS}"

touch "${SENTINEL}"
log "sentinela criada em ${SENTINEL}, aguardando indefinidamente (healthcheck le este arquivo)."
exec sleep infinity
