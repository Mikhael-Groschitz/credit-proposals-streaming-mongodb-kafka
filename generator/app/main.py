from __future__ import annotations

import logging
import random
import signal
import sys
import threading
from types import FrameType

from .config import Config
from .generator import GeradorDeCarga
from .repository import RepositorioPropostas


def _configurar_logging(nivel: str) -> None:
    logging.basicConfig(
        level=nivel,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
        stream=sys.stdout,
    )


def main() -> None:
    config = Config.from_env()
    _configurar_logging(config.log_level)
    logger = logging.getLogger("gerador")

    if config.random_seed is not None:
        random.seed(config.random_seed)
        logger.info("seed fixa configurada: %d", config.random_seed)

    logger.info(
        "iniciando gerador: %.2f eventos/s, mongo=%s db=%s colecao=%s",
        config.events_per_second, config.mongo_uri, config.mongo_db, config.mongo_collection,
    )

    repositorio = RepositorioPropostas(config.mongo_uri, config.mongo_db, config.mongo_collection)
    gerador = GeradorDeCarga(config, repositorio)

    parar = threading.Event()

    def _tratar_sinal(signum: int, _frame: FrameType | None) -> None:
        logger.info("sinal %s recebido, encerrando apos o evento em andamento...", signum)
        parar.set()

    signal.signal(signal.SIGTERM, _tratar_sinal)
    signal.signal(signal.SIGINT, _tratar_sinal)

    try:
        gerador.executar(parar)
    finally:
        repositorio.fechar()
        logger.info("gerador encerrado.")


if __name__ == "__main__":
    main()
