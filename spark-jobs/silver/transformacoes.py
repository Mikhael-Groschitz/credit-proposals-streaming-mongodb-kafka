from __future__ import annotations

import json

CAMPOS_COMUNS = frozenset(
    {
        "_id",
        "id_proposta",
        "tipo_produto",
        "cpf_cliente",
        "nome_cliente",
        "valor_solicitado",
        "status",
        "data_criacao",
        "data_atualizacao",
    }
)


def extrair_campos_especificos(documento_json: str | None) -> str | None:
    if documento_json is None:
        return None
    documento = json.loads(documento_json)
    especificos = {chave: valor for chave, valor in documento.items() if chave not in CAMPOS_COMUNS}
    return json.dumps(especificos, ensure_ascii=False)


def eh_transicao_de_status(operation_type: str, update_description_json: str | None) -> bool:
    if operation_type in ("insert", "delete", "replace"):
        return True
    if operation_type != "update":
        return False
    if update_description_json is None:
        return False
    update_description = json.loads(update_description_json)
    campos_alterados = update_description.get("updatedFields", {})
    return "status" in campos_alterados
