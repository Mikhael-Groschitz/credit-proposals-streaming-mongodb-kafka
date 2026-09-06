from __future__ import annotations

import os
from dataclasses import dataclass


def _int_env(nome: str, padrao: int) -> int:
    valor = os.environ.get(nome)
    if valor is None or valor == "":
        return padrao
    try:
        return int(valor)
    except ValueError as exc:
        raise ValueError(
            f"variavel de ambiente {nome!r} precisa ser um inteiro, recebeu {valor!r}"
        ) from exc


@dataclass(frozen=True)
class Config:
    mongo_host: str
    mongo_port: int
    mongo_replica_set: str
    mongo_db: str
    mongo_collection: str

    kafka_bootstrap_servers: str
    kafka_topic: str
    kafka_delivery_timeout_ms: int
    kafka_request_timeout_ms: int

    checkpoint_collection: str
    checkpoint_id: str

    batch_max_size: int
    batch_max_intervalo_ms: int

    retry_backoff_inicial_ms: int
    retry_backoff_maximo_ms: int

    log_level: str

    @property
    def mongo_uri(self) -> str:
        return f"mongodb://{self.mongo_host}:{self.mongo_port}/?replicaSet={self.mongo_replica_set}"

    @classmethod
    def from_env(cls) -> "Config":
        batch_max_size = _int_env("BATCH_MAX_SIZE", 50)
        if batch_max_size <= 0:
            raise ValueError("BATCH_MAX_SIZE precisa ser maior que zero")

        batch_max_intervalo_ms = _int_env("BATCH_MAX_INTERVALO_MS", 2000)
        if batch_max_intervalo_ms <= 0:
            raise ValueError("BATCH_MAX_INTERVALO_MS precisa ser maior que zero")

        kafka_delivery_timeout_ms = _int_env("KAFKA_DELIVERY_TIMEOUT_MS", 15000)
        if kafka_delivery_timeout_ms <= 0:
            raise ValueError("KAFKA_DELIVERY_TIMEOUT_MS precisa ser maior que zero")

        kafka_request_timeout_ms = _int_env("KAFKA_REQUEST_TIMEOUT_MS", 5000)
        if kafka_request_timeout_ms <= 0:
            raise ValueError("KAFKA_REQUEST_TIMEOUT_MS precisa ser maior que zero")

        retry_backoff_inicial_ms = _int_env("RETRY_BACKOFF_INICIAL_MS", 1000)
        if retry_backoff_inicial_ms <= 0:
            raise ValueError("RETRY_BACKOFF_INICIAL_MS precisa ser maior que zero")

        retry_backoff_maximo_ms = _int_env("RETRY_BACKOFF_MAXIMO_MS", 30000)
        if retry_backoff_maximo_ms < retry_backoff_inicial_ms:
            raise ValueError(
                "RETRY_BACKOFF_MAXIMO_MS precisa ser maior ou igual a RETRY_BACKOFF_INICIAL_MS"
            )

        return cls(
            mongo_host=os.environ.get("MONGO_HOST", "mongo"),
            mongo_port=int(os.environ.get("MONGO_PORT", "27017")),
            mongo_replica_set=os.environ.get("MONGO_REPLICA_SET", "rs0"),
            mongo_db=os.environ.get("MONGO_DB", "creditodb"),
            mongo_collection=os.environ.get("MONGO_COLLECTION", "propostas"),
            kafka_bootstrap_servers=os.environ.get("KAFKA_BOOTSTRAP_INTERNO", "kafka:19092"),
            kafka_topic=os.environ.get("KAFKA_TOPIC_PROPOSTAS", "propostas.cdc"),
            kafka_delivery_timeout_ms=kafka_delivery_timeout_ms,
            kafka_request_timeout_ms=kafka_request_timeout_ms,
            checkpoint_collection=os.environ.get("CHECKPOINT_COLLECTION", "checkpoints"),
            checkpoint_id=os.environ.get("CHECKPOINT_ID", "conector-propostas"),
            batch_max_size=batch_max_size,
            batch_max_intervalo_ms=batch_max_intervalo_ms,
            retry_backoff_inicial_ms=retry_backoff_inicial_ms,
            retry_backoff_maximo_ms=retry_backoff_maximo_ms,
            log_level=os.environ.get("LOG_LEVEL", "INFO").upper(),
        )
