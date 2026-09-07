from transformacoes import estado_e_impossivel, transicao_e_valida, valor_fora_de_faixa


def test_status_conhecido_nao_e_impossivel():
    assert estado_e_impossivel("em_analise") is False
    assert estado_e_impossivel("paga") is False
    assert estado_e_impossivel("deletada") is False


def test_status_desconhecido_e_impossivel():
    assert estado_e_impossivel("aprovado_com_erro_de_digitacao") is True


def test_status_nulo_e_impossivel():
    assert estado_e_impossivel(None) is True


def test_valor_dentro_da_faixa_do_produto():
    assert valor_fora_de_faixa(25_000.0, "consignado") is False
    assert valor_fora_de_faixa(10_000.0, "cartao") is False
    assert valor_fora_de_faixa(15_000.0, "fgts") is False


def test_valor_abaixo_da_faixa_e_fora_de_faixa():
    assert valor_fora_de_faixa(100.0, "consignado") is True


def test_valor_acima_da_faixa_e_fora_de_faixa():
    assert valor_fora_de_faixa(999_999.0, "cartao") is True


def test_valor_ou_tipo_nulo_e_fora_de_faixa():
    assert valor_fora_de_faixa(None, "cartao") is True
    assert valor_fora_de_faixa(1_000.0, None) is True


def test_tipo_produto_desconhecido_e_fora_de_faixa():
    assert valor_fora_de_faixa(1_000.0, "consorcio") is True


def test_primeira_entrada_sem_predecessor_e_sempre_valida():
    assert transicao_e_valida(None, "em_analise") is True


def test_repeticao_do_mesmo_status_e_valida():
    assert transicao_e_valida("em_analise", "em_analise") is True


def test_transicao_prevista_no_ciclo_de_vida_e_valida():
    assert transicao_e_valida("em_analise", "aprovada") is True
    assert transicao_e_valida("pendente_documentacao", "em_analise") is True
    assert transicao_e_valida("aprovada", "paga") is True


def test_transicao_fora_do_ciclo_de_vida_e_invalida():
    assert transicao_e_valida("recusada", "aprovada") is False
    assert transicao_e_valida("paga", "cancelada") is False
    assert transicao_e_valida("em_analise", "paga") is False


def test_transicao_para_deletada_e_sempre_valida_de_qualquer_estado():
    assert transicao_e_valida("em_analise", "deletada") is True
    assert transicao_e_valida("paga", "deletada") is True
    assert transicao_e_valida("recusada", "deletada") is True
