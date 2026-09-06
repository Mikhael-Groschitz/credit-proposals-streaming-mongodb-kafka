from bson import Timestamp

from app.eventos import construir_evento


def test_insert_extrai_id_proposta_do_full_document():
    change = {
        "_id": {"_data": "8265..."},
        "operationType": "insert",
        "documentKey": {"_id": "abc123"},
        "fullDocument": {"id_proposta": "prop-1", "status": "em_analise"},
    }

    evento = construir_evento(change)

    assert evento.id_proposta == "prop-1"
    assert evento.operation_type == "insert"
    assert evento.envelope["chave_derivada_de_fallback"] is False
    assert evento.envelope["full_document"]["status"] == "em_analise"


def test_update_extrai_id_proposta_do_full_document_via_update_lookup():
    change = {
        "_id": {"_data": "8266..."},
        "operationType": "update",
        "documentKey": {"_id": "abc123"},
        "fullDocument": {"id_proposta": "prop-1", "status": "aprovada"},
        "updateDescription": {"updatedFields": {"status": "aprovada"}},
    }

    evento = construir_evento(change)

    assert evento.id_proposta == "prop-1"
    assert evento.envelope["update_description"] == {"updatedFields": {"status": "aprovada"}}


def test_delete_extrai_id_proposta_do_full_document_before_change():
    change = {
        "_id": {"_data": "8267..."},
        "operationType": "delete",
        "documentKey": {"_id": "abc123"},
        "fullDocumentBeforeChange": {"id_proposta": "prop-2", "status": "em_analise"},
    }

    evento = construir_evento(change)

    assert evento.id_proposta == "prop-2"
    assert evento.envelope["chave_derivada_de_fallback"] is False
    assert evento.envelope["full_document"] is None


def test_sem_id_proposta_disponivel_usa_document_key_como_fallback():
    change = {
        "_id": {"_data": "8268..."},
        "operationType": "delete",
        "documentKey": {"_id": "abc123"},
    }

    evento = construir_evento(change)

    assert evento.id_proposta == "abc123"
    assert evento.envelope["chave_derivada_de_fallback"] is True


def test_cluster_time_epoch_extraido_do_timestamp_bson():
    change = {
        "_id": {"_data": "8270..."},
        "operationType": "insert",
        "documentKey": {"_id": "abc123"},
        "fullDocument": {"id_proposta": "prop-4"},
        "clusterTime": Timestamp(1788710806, 5),
    }

    evento = construir_evento(change)

    assert evento.envelope["cluster_time_epoch"] == 1788710806


def test_cluster_time_epoch_e_none_quando_ausente():
    change = {
        "_id": {"_data": "8271..."},
        "operationType": "insert",
        "documentKey": {"_id": "abc123"},
        "fullDocument": {"id_proposta": "prop-5"},
    }

    evento = construir_evento(change)

    assert evento.envelope["cluster_time_epoch"] is None


def test_resume_token_do_evento_e_o_id_do_change_stream():
    change = {
        "_id": {"_data": "8269..."},
        "operationType": "insert",
        "documentKey": {"_id": "abc123"},
        "fullDocument": {"id_proposta": "prop-3"},
    }

    evento = construir_evento(change)

    assert evento.resume_token == {"_data": "8269..."}
