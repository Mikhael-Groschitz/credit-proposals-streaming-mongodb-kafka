from __future__ import annotations

import logging
import signal
import sys
import threading
from types import FrameType

from .conector import ConectorChangeStreams
from .config import Config


def _configurar_logging(nivel: str) -> None:
    logging.basicConfig(
        level=nivel,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
        stream=sys.stdout,
    )


def main() -> None:
    config = Config.from_env()
    _configurar_logging(config.log_level)
    logger = logging.getLogger("conector")

    logger.info(
        "iniciando conector: mongo=%s db=%s colecao=%s -> kafka=%s topico=%s",
        config.mongo_uri,
        config.mongo_db,
        config.mongo_collection,
        config.kafka_bootstrap_servers,
        config.kafka_topic,
    )

    conector = ConectorChangeStreams(config)

    parar = threading.Event()

    def _tratar_sinal(signum: int, _frame: FrameType | None) -> None:
        logger.info("sinal %s recebido, encerrando apos o lote em andamento...", signum)
        parar.set()

    signal.signal(signal.SIGTERM, _tratar_sinal)
    signal.signal(signal.SIGINT, _tratar_sinal)

    try:
        conector.executar(parar)
    finally:
        conector.fechar()
        logger.info("conector encerrado.")


if __name__ == "__main__":
    main()
