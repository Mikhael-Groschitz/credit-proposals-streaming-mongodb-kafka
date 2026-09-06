import pytest

from app.state_machine import TRANSICOES, eh_terminal, proximo_status


@pytest.mark.parametrize("status", ["recusada", "paga", "cancelada"])
def test_estados_terminais_nao_tem_transicao(status):
    assert proximo_status(status) is None
    assert eh_terminal(status)


@pytest.mark.parametrize("status", ["em_analise", "pendente_documentacao", "aprovada"])
def test_estados_nao_terminais_tem_transicao_valida(status):
    assert not eh_terminal(status)
    destino = proximo_status(status)
    assert destino in TRANSICOES[status]


def test_status_desconhecido_levanta_erro_em_proximo_status():
    with pytest.raises(ValueError):
        proximo_status("status_inexistente")


def test_status_desconhecido_levanta_erro_em_eh_terminal():
    with pytest.raises(ValueError):
        eh_terminal("status_inexistente")


def test_aprovada_so_pode_ir_para_paga_ou_cancelada():
    assert set(TRANSICOES["aprovada"]) == {"paga", "cancelada"}


def test_pendente_documentacao_pode_voltar_para_em_analise():
    assert "em_analise" in TRANSICOES["pendente_documentacao"]


def test_amostragem_estatistica_cobre_todos_os_destinos_de_em_analise():
    destinos_observados = {proximo_status("em_analise") for _ in range(500)}
    assert destinos_observados == set(TRANSICOES["em_analise"])
