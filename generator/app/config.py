from __future__ import annotations

import os
from dataclasses import dataclass


def _float_env(nome: str, padrao: float) -> float:
    valor = os.environ.get(nome)
    if valor is None or valor == "":
        return padrao
    try:
        return float(valor)
    except ValueError as exc:
        raise ValueError(
            f"variavel de ambiente {nome!r} precisa ser um numero, recebeu {valor!r}"
        ) from exc


def _int_env_opcional(nome: str) -> int | None:
    valor = os.environ.get(nome)
    if valor is None or valor == "":
        return None
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

    events_per_second: float
    ratio_consignado: float
    ratio_cartao: float
    ratio_fgts: float
    ratio_new_vs_transition: float
    ratio_delete: float
    ratio_correcao: float
    random_seed: int | None
    log_level: str

    @property
    def mongo_uri(self) -> str:
        return f"mongodb://{self.mongo_host}:{self.mongo_port}/?replicaSet={self.mongo_replica_set}"

    @property
    def pesos_tipo_produto(self) -> dict[str, float]:
        return {
            "consignado": self.ratio_consignado,
            "cartao": self.ratio_cartao,
            "fgts": self.ratio_fgts,
        }

    @classmethod
    def from_env(cls) -> "Config":
        events_per_second = _float_env("EVENTS_PER_SECOND", 5.0)
        if events_per_second <= 0:
            raise ValueError("EVENTS_PER_SECOND precisa ser maior que zero")

        ratio_consignado = _float_env("RATIO_CONSIGNADO", 0.5)
        ratio_cartao = _float_env("RATIO_CARTAO", 0.3)
        ratio_fgts = _float_env("RATIO_FGTS", 0.2)
        if min(ratio_consignado, ratio_cartao, ratio_fgts) < 0:
            raise ValueError("RATIO_CONSIGNADO/RATIO_CARTAO/RATIO_FGTS nao podem ser negativos")
        if ratio_consignado + ratio_cartao + ratio_fgts <= 0:
            raise ValueError(
                "pelo menos um de RATIO_CONSIGNADO/RATIO_CARTAO/RATIO_FGTS precisa ser positivo"
            )

        ratio_new_vs_transition = _float_env("RATIO_NEW_VS_TRANSITION", 0.3)
        ratio_delete = _float_env("RATIO_DELETE", 0.02)
        ratio_correcao = _float_env("RATIO_CORRECAO", 0.05)
        for nome, valor in (
            ("RATIO_NEW_VS_TRANSITION", ratio_new_vs_transition),
            ("RATIO_DELETE", ratio_delete),
            ("RATIO_CORRECAO", ratio_correcao),
        ):
            if not 0 <= valor <= 1:
                raise ValueError(f"{nome} precisa estar entre 0 e 1, recebeu {valor}")

        return cls(
            mongo_host=os.environ.get("MONGO_HOST", "mongo"),
            mongo_port=int(os.environ.get("MONGO_PORT", "27017")),
            mongo_replica_set=os.environ.get("MONGO_REPLICA_SET", "rs0"),
            mongo_db=os.environ.get("MONGO_DB", "creditodb"),
            mongo_collection=os.environ.get("MONGO_COLLECTION", "propostas"),
            events_per_second=events_per_second,
            ratio_consignado=ratio_consignado,
            ratio_cartao=ratio_cartao,
            ratio_fgts=ratio_fgts,
            ratio_new_vs_transition=ratio_new_vs_transition,
            ratio_delete=ratio_delete,
            ratio_correcao=ratio_correcao,
            random_seed=_int_env_opcional("RANDOM_SEED"),
            log_level=os.environ.get("LOG_LEVEL", "INFO").upper(),
        )
