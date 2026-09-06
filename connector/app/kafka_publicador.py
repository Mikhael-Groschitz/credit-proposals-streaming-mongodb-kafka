from __future__ import annotations

from confluent_kafka import Producer

from .eventos import EventoAlteracao
from .serializacao import serializar_envelope


class FalhaDePublicacao(Exception):
    pass


class PublicadorKafka:
    def __init__(self, topico: str, producer) -> None:
        self._topico = topico
        self._producer = producer

    @classmethod
    def criar(
        cls,
        bootstrap_servers: str,
        topico: str,
        delivery_timeout_ms: int,
        request_timeout_ms: int,
    ) -> "PublicadorKafka":
        producer = Producer(
            {
                "bootstrap.servers": bootstrap_servers,
                "acks": "all",
                "enable.idempotence": True,
                "delivery.timeout.ms": delivery_timeout_ms,
                "request.timeout.ms": request_timeout_ms,
            }
        )
        return cls(topico, producer)

    def publicar_lote(self, eventos: list[EventoAlteracao]) -> None:
        resultados: list[dict] = [{} for _ in eventos]

        def _fazer_callback(indice: int):
            def _callback(err, _msg):
                resultados[indice]["err"] = err

            return _callback

        for indice, evento in enumerate(eventos):
            self._producer.produce(
                self._topico,
                key=evento.id_proposta.encode("utf-8"),
                value=serializar_envelope(evento.envelope),
                on_delivery=_fazer_callback(indice),
            )

        pendentes = self._producer.flush(timeout=30)
        if pendentes > 0:
            raise FalhaDePublicacao(
                f"{pendentes} mensagem(ns) nao confirmadas apos timeout de flush"
            )

        falhas = [r["err"] for r in resultados if r.get("err") is not None]
        if falhas:
            raise FalhaDePublicacao(
                f"{len(falhas)} de {len(eventos)} mensagem(ns) falharam na entrega: {falhas[0]}"
            )

    def fechar(self) -> None:
        self._producer.flush(timeout=10)
