#!/usr/bin/env sh
set -eu

MINIO_HOST="${MINIO_HOST:-minio}"
MINIO_PORT="${MINIO_PORT:-9000}"
MINIO_ROOT_USER="${MINIO_ROOT_USER:?MINIO_ROOT_USER precisa estar definido}"
MINIO_ROOT_PASSWORD="${MINIO_ROOT_PASSWORD:?MINIO_ROOT_PASSWORD precisa estar definido}"
MINIO_BUCKET_BRONZE="${MINIO_BUCKET_BRONZE:-bronze}"
MINIO_BUCKET_SILVER="${MINIO_BUCKET_SILVER:-silver}"
MINIO_BUCKET_GOLD="${MINIO_BUCKET_GOLD:-gold}"
SENTINEL=/tmp/init-done
MAX_TENTATIVAS=60
INTERVALO_SEGUNDOS=2

log() { echo "[minio-init] $*"; }

log "aguardando alias local apontar para http://${MINIO_HOST}:${MINIO_PORT}..."
tentativa=0
until mc alias set local "http://${MINIO_HOST}:${MINIO_PORT}" "${MINIO_ROOT_USER}" "${MINIO_ROOT_PASSWORD}" >/dev/null 2>&1; do
    tentativa=$((tentativa + 1))
    if [ "${tentativa}" -ge "${MAX_TENTATIVAS}" ]; then
        log "ERRO: nao foi possivel autenticar no MinIO apos ${MAX_TENTATIVAS} tentativas."
        exit 1
    fi
    sleep "${INTERVALO_SEGUNDOS}"
done
log "alias configurado."

for bucket in "${MINIO_BUCKET_BRONZE}" "${MINIO_BUCKET_SILVER}" "${MINIO_BUCKET_GOLD}"; do
    log "garantindo bucket 'local/${bucket}'..."
    mc mb --ignore-existing "local/${bucket}"
done

log "buckets prontos:"
mc ls local

touch "${SENTINEL}"
log "sentinela criada em ${SENTINEL}, aguardando indefinidamente (healthcheck le este arquivo)."
exec sleep infinity
