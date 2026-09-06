from __future__ import annotations

import logging
import threading
import time

from pymongo import MongoClient
from pymongo.errors import OperationFailure, PyMongoError

from .backoff import proximo_backoff_ms
from .checkpoint import RepositorioCheckpoint
from .config import Config
from .erros import ColecaoInvalidadaError, ResumeTokenInvalidoError
from .eventos import EventoAlteracao, construir_evento
from .kafka_publicador import FalhaDePublicacao, PublicadorKafka

logger = logging.getLogger(__name__)

CODIGOS_RESUME_TOKEN_INVALIDO = {260, 280, 286, 346, 351, 464}


class ConectorChangeStreams:
    def __init__(self, config: Config) -> None:
        self._config = config
        self._client = MongoClient(config.mongo_uri)
        self._db = self._client[config.mongo_db]
        self._colecao = self._db[config.mongo_collection]
        self._checkpoint = RepositorioCheckpoint(
            self._db, config.checkpoint_collection, config.checkpoint_id
        )
        self._publicador = PublicadorKafka.criar(
            config.kafka_bootstrap_servers,
            config.kafka_topic,
            config.kafka_delivery_timeout_ms,
            config.kafka_request_timeout_ms,
        )

    def executar(self, parar: threading.Event) -> None:
        backoff_ms = self._config.retry_backoff_inicial_ms
        while not parar.is_set():
            try:
                self._executar_sessao(parar)
                return
            except (ResumeTokenInvalidoError, ColecaoInvalidadaError):
                raise
            except PyMongoError as exc:
                logger.error(
                    "MongoDB indisponivel (%s), tentando novamente em %.1fs...",
                    exc,
                    backoff_ms / 1000,
                )
                parar.wait(timeout=backoff_ms / 1000)
                backoff_ms = proximo_backoff_ms(backoff_ms, self._config.retry_backoff_maximo_ms)

    def _abrir_stream(self, resume_token: dict | None):
        if resume_token is None:
            return self._colecao.watch(
                full_document="updateLookup",
                full_document_before_change="whenAvailable",
                max_await_time_ms=1000,
            )
        try:
            return self._colecao.watch(
                resume_after=resume_token,
                full_document="updateLookup",
                full_document_before_change="whenAvailable",
                max_await_time_ms=1000,
            )
        except OperationFailure as exc:
            raise ResumeTokenInvalidoError(
                f"nao foi possivel retomar a partir do resume token persistido "
                f"(code={exc.code}, {exc.details.get('errmsg', exc) if exc.details else exc}). "
                f"E necessario um reload completo: remova o checkpoint "
                f"'{self._config.checkpoint_id}' na colecao "
                f"'{self._config.checkpoint_collection}' apos validar o impacto."
            ) from exc

    def _executar_sessao(self, parar: threading.Event) -> None:
        resume_token = self._checkpoint.carregar_resume_token()
        if resume_token is not None:
            logger.info("retomando a partir do resume token persistido")
        else:
            logger.info("nenhum checkpoint encontrado, iniciando a partir de agora")

        with self._abrir_stream(resume_token) as stream:
            lote: list[EventoAlteracao] = []
            inicio_lote = time.monotonic()

            while not parar.is_set():
                try:
                    change = stream.try_next()
                except OperationFailure as exc:
                    if exc.code in CODIGOS_RESUME_TOKEN_INVALIDO:
                        raise ResumeTokenInvalidoError(
                            f"resume token invalido/expirado durante a leitura (code={exc.code})."
                        ) from exc
                    raise

                if change is not None:
                    if change["operationType"] == "invalidate":
                        raise ColecaoInvalidadaError(
                            "a colecao monitorada foi removida ou renomeada; encerrando."
                        )
                    lote.append(construir_evento(change))

                agora = time.monotonic()
                tempo_no_lote_ms = (agora - inicio_lote) * 1000
                deve_esvaziar = lote and (
                    len(lote) >= self._config.batch_max_size
                    or tempo_no_lote_ms >= self._config.batch_max_intervalo_ms
                )
                if deve_esvaziar and self._esvaziar_lote(lote, parar):
                    lote = []
                    inicio_lote = time.monotonic()

            if lote:
                self._esvaziar_lote(lote, parar)

    def _esvaziar_lote(self, lote: list[EventoAlteracao], parar: threading.Event) -> bool:
        backoff_ms = self._config.retry_backoff_inicial_ms
        while not parar.is_set():
            try:
                self._publicador.publicar_lote(lote)
                self._checkpoint.salvar_resume_token(lote[-1].resume_token)
                logger.info("lote de %d evento(s) publicado e confirmado", len(lote))
                return True
            except FalhaDePublicacao as exc:
                logger.error(
                    "falha ao publicar lote no Kafka (%s), tentando novamente em %.1fs...",
                    exc,
                    backoff_ms / 1000,
                )
                parar.wait(timeout=backoff_ms / 1000)
                backoff_ms = proximo_backoff_ms(backoff_ms, self._config.retry_backoff_maximo_ms)
        return False

    def fechar(self) -> None:
        self._publicador.fechar()
        self._client.close()
