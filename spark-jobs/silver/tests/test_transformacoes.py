import json

from transformacoes import (
    eh_transicao_de_status,
    extrair_campos_especificos,
    mesclar_full_document_com_update_description,
)


def test_extrai_campos_especificos_de_consignado():
    documento = {
        "_id": {"$oid": "abc"},
        "id_proposta": "prop-1",
        "tipo_produto": "consignado",
        "cpf_cliente": "111",
        "nome_cliente": "Fulano",
        "valor_solicitado": 100.0,
        "status": "em_analise",
        "data_criacao": {"$date": "2026-01-01T00:00:00Z"},
        "data_atualizacao": {"$date": "2026-01-01T00:00:00Z"},
        "matricula": "123456",
        "orgao_convenio": "INSS",
        "prazo_meses": 24,
    }

    resultado = json.loads(extrair_campos_especificos(json.dumps(documento)))

    assert resultado == {"matricula": "123456", "orgao_convenio": "INSS", "prazo_meses": 24}


def test_extrai_campos_especificos_de_cartao_nao_vaza_campos_de_consignado():
    documento = {
        "id_proposta": "prop-2",
        "tipo_produto": "cartao",
        "status": "em_analise",
        "limite_solicitado": 5000.0,
        "bandeira": "Visa",
    }

    resultado = json.loads(extrair_campos_especificos(json.dumps(documento)))

    assert resultado == {"limite_solicitado": 5000.0, "bandeira": "Visa"}
    assert "matricula" not in resultado


def test_documento_nulo_retorna_none():
    assert extrair_campos_especificos(None) is None


def test_documento_so_com_campos_comuns_retorna_dict_vazio():
    documento = {"id_proposta": "prop-3", "status": "paga"}

    resultado = json.loads(extrair_campos_especificos(json.dumps(documento)))

    assert resultado == {}


def test_insert_e_sempre_transicao():
    assert eh_transicao_de_status("insert", None) is True


def test_delete_e_sempre_transicao():
    assert eh_transicao_de_status("delete", None) is True


def test_replace_e_sempre_transicao():
    assert eh_transicao_de_status("replace", None) is True


def test_update_com_status_no_updated_fields_e_transicao():
    update_description = {"updatedFields": {"status": "aprovada", "data_atualizacao": "..."}}

    assert eh_transicao_de_status("update", json.dumps(update_description)) is True


def test_update_sem_status_no_updated_fields_nao_e_transicao():
    update_description = {"updatedFields": {"valor_solicitado": 999.0, "data_atualizacao": "..."}}

    assert eh_transicao_de_status("update", json.dumps(update_description)) is False


def test_update_sem_update_description_nao_e_transicao():
    assert eh_transicao_de_status("update", None) is False


def test_mesclar_sobrescreve_full_document_com_updated_fields():
    documento_fonte = json.dumps({"id_proposta": "prop-4", "status": "paga", "valor_solicitado": 100.0})
    update_description = json.dumps({"updatedFields": {"status": "aprovada"}})

    resultado = json.loads(mesclar_full_document_com_update_description(documento_fonte, update_description))

    assert resultado["status"] == "aprovada"
    assert resultado["valor_solicitado"] == 100.0


def test_mesclar_sem_update_description_mantem_full_document():
    documento_fonte = json.dumps({"id_proposta": "prop-5", "status": "em_analise"})

    resultado = json.loads(mesclar_full_document_com_update_description(documento_fonte, None))

    assert resultado["status"] == "em_analise"


def test_mesclar_documento_nulo_retorna_none():
    assert mesclar_full_document_com_update_description(None, json.dumps({"updatedFields": {}})) is None
