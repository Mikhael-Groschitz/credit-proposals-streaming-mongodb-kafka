from __future__ import annotations

import logging
import random
import threading
import time
from dataclasses import dataclass

from .config import Config
from .factories import (
    gerar_proposta,
    novo_cpf_cliente,
    novo_nome_cliente,
    novo_valor_solicitado,
    sortear_tipo_produto,
)
from .repository import RepositorioPropostas
from .state_machine import eh_terminal, proximo_status

logger = logging.getLogger(__name__)


@dataclass
class _PropostaAtiva:
    tipo_produto: str
    status: str
    marcada_para_delecao: bool = False


class GeradorDeCarga:
    def __init__(self, config: Config, repositorio: RepositorioPropostas) -> None:
        self._config = config
        self._repositorio = repositorio
        self._propostas_ativas: dict[str, _PropostaAtiva] = {}

    def executar(self, parar: threading.Event) -> None:
        intervalo = 1.0 / self._config.events_per_second
        while not parar.is_set():
            inicio = time.monotonic()
            try:
                self.executar_um_evento()
            except Exception:
                logger.exception("falha ao processar evento, continuando no proximo tick")
            decorrido = time.monotonic() - inicio
            parar.wait(timeout=max(0.0, intervalo - decorrido))

    def executar_um_evento(self) -> None:
        deve_criar = (
            not self._propostas_ativas
            or random.random() < self._config.ratio_new_vs_transition
        )
        if deve_criar:
            self._criar_proposta()
            return

        id_proposta = random.choice(list(self._propostas_ativas.keys()))
        proposta = self._propostas_ativas[id_proposta]

        if random.random() < self._config.ratio_correcao:
            self._aplicar_correcao(id_proposta)
            return

        if proposta.marcada_para_delecao:
            self._deletar(id_proposta)
            return

        self._transicionar(id_proposta, proposta)

    def _criar_proposta(self) -> None:
        tipo_produto = sortear_tipo_produto(self._config.pesos_tipo_produto)
        documento = gerar_proposta(tipo_produto)
        self._repositorio.inserir(documento)

        marcada_para_delecao = random.random() < self._config.ratio_delete
        self._propostas_ativas[documento["id_proposta"]] = _PropostaAtiva(
            tipo_produto=tipo_produto,
            status=documento["status"],
            marcada_para_delecao=marcada_para_delecao,
        )
        logger.info(
            "proposta criada id=%s tipo=%s status=%s marcada_para_delecao=%s",
            documento["id_proposta"], tipo_produto, documento["status"], marcada_para_delecao,
        )

    def _transicionar(self, id_proposta: str, proposta: _PropostaAtiva) -> None:
        novo_status = proximo_status(proposta.status)
        if novo_status is None:
            del self._propostas_ativas[id_proposta]
            return

        self._repositorio.atualizar_status(id_proposta, novo_status)
        proposta.status = novo_status
        logger.info("proposta transicionou id=%s novo_status=%s", id_proposta, novo_status)

        if eh_terminal(novo_status):
            del self._propostas_ativas[id_proposta]

    def _aplicar_correcao(self, id_proposta: str) -> None:
        campo, valor = self._sortear_campo_correcao()
        self._repositorio.aplicar_correcao_retroativa(id_proposta, {campo: valor})
        logger.info("correcao retroativa aplicada id=%s campo=%s", id_proposta, campo)

    @staticmethod
    def _sortear_campo_correcao() -> tuple[str, object]:
        campo = random.choice(("valor_solicitado", "nome_cliente", "cpf_cliente"))
        if campo == "valor_solicitado":
            return campo, novo_valor_solicitado()
        if campo == "nome_cliente":
            return campo, novo_nome_cliente()
        return campo, novo_cpf_cliente()

    def _deletar(self, id_proposta: str) -> None:
        self._repositorio.deletar(id_proposta)
        del self._propostas_ativas[id_proposta]
        logger.info("proposta deletada (erro operacional simulado) id=%s", id_proposta)
