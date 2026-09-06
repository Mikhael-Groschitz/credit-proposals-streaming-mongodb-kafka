#!/usr/bin/env bash
set -euo pipefail

MONGO_HOST="${MONGO_HOST:-mongo}"
MONGO_PORT="${MONGO_PORT:-27017}"
MONGO_REPLICA_SET="${MONGO_REPLICA_SET:-rs0}"
SENTINEL=/tmp/init-done
MAX_TENTATIVAS=60
INTERVALO_SEGUNDOS=2

log() { echo "[mongo-init] $*"; }

mongosh_eval() {
    mongosh --host "${MONGO_HOST}" --port "${MONGO_PORT}" --quiet --eval "$1"
}

log "aguardando mongod em ${MONGO_HOST}:${MONGO_PORT} aceitar conexoes..."
tentativa=0
until mongosh_eval "db.adminCommand('ping').ok" >/dev/null 2>&1; do
    tentativa=$((tentativa + 1))
    if [ "${tentativa}" -ge "${MAX_TENTATIVAS}" ]; then
        log "ERRO: mongod nao respondeu ao ping apos ${MAX_TENTATIVAS} tentativas."
        exit 1
    fi
    sleep "${INTERVALO_SEGUNDOS}"
done
log "mongod respondendo."

status_atual="$(mongosh_eval "
try {
    JSON.stringify(rs.status().ok)
} catch (e) {
    'no-replset'
}
" | tail -n1)"

if [ "${status_atual}" = "no-replset" ]; then
    log "replica set ainda nao existe, iniciando '${MONGO_REPLICA_SET}' com membro ${MONGO_HOST}:${MONGO_PORT}..."
    mongosh_eval "
        rs.initiate({
            _id: '${MONGO_REPLICA_SET}',
            members: [{ _id: 0, host: '${MONGO_HOST}:${MONGO_PORT}' }]
        })
    "
else
    log "replica set '${MONGO_REPLICA_SET}' ja existe, pulando rs.initiate()."
fi

log "aguardando o no assumir PRIMARY..."
tentativa=0
until [ "$(mongosh_eval 'rs.status().myState' | tail -n1)" = "1" ]; do
    tentativa=$((tentativa + 1))
    if [ "${tentativa}" -ge "${MAX_TENTATIVAS}" ]; then
        log "ERRO: no nao assumiu PRIMARY apos ${MAX_TENTATIVAS} tentativas."
        mongosh_eval "rs.status()"
        exit 1
    fi
    sleep "${INTERVALO_SEGUNDOS}"
done
log "replica set pronto (PRIMARY eleito)."

touch "${SENTINEL}"
log "sentinela criada em ${SENTINEL}, aguardando indefinidamente (healthcheck le este arquivo)."
exec sleep infinity
