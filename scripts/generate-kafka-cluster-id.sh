#!/usr/bin/env bash
set -euo pipefail

IMAGEM_KAFKA="${1:-apache/kafka:4.3.1}"

docker run --rm "${IMAGEM_KAFKA}" /opt/kafka/bin/kafka-storage.sh random-uuid
