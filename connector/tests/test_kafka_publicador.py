import pytest

from app.eventos import EventoAlteracao
from app.kafka_publicador import FalhaDePublicacao, PublicadorKafka


class ErroFalso:
    def __str__(self):
        return "erro-simulado"


class ProdutorFalso:
    def __init__(self, falha_no_indice=None, pendentes_no_flush=0):
        self.mensagens: list[tuple] = []
        self._falha_no_indice = falha_no_indice
        self._pendentes_no_flush = pendentes_no_flush

    def produce(self, topico, key, value, on_delivery):
        indice = len(self.mensagens)
        self.mensagens.append((topico, key, value))
        erro = ErroFalso() if indice == self._falha_no_indice else None
        on_delivery(erro, None)

    def flush(self, timeout=None):
        return self._pendentes_no_flush


def _evento(id_proposta: str) -> EventoAlteracao:
    return EventoAlteracao(
        id_proposta=id_proposta,
        operation_type="insert",
        resume_token={"_data": id_proposta},
        envelope={"id_proposta": id_proposta},
    )


def test_publicar_lote_com_sucesso_nao_levanta_erro():
    produtor = ProdutorFalso()
    publicador = PublicadorKafka("propostas.cdc", produtor)

    publicador.publicar_lote([_evento("prop-1"), _evento("prop-2")])

    assert len(produtor.mensagens) == 2
    assert produtor.mensagens[0][1] == b"prop-1"


def test_falha_de_entrega_de_uma_mensagem_levanta_falha_de_publicacao():
    produtor = ProdutorFalso(falha_no_indice=1)
    publicador = PublicadorKafka("propostas.cdc", produtor)

    with pytest.raises(FalhaDePublicacao):
        publicador.publicar_lote([_evento("prop-1"), _evento("prop-2")])


def test_mensagens_pendentes_apos_flush_levanta_falha_de_publicacao():
    produtor = ProdutorFalso(pendentes_no_flush=1)
    publicador = PublicadorKafka("propostas.cdc", produtor)

    with pytest.raises(FalhaDePublicacao):
        publicador.publicar_lote([_evento("prop-1")])
