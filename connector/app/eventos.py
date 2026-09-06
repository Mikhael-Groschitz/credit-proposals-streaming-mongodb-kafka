from __future__ import annotations

from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True)
class EventoAlteracao:
    id_proposta: str
    operation_type: str
    resume_token: dict
    envelope: dict[str, Any]


def _extrair_id_proposta(change: dict) -> tuple[str, bool]:
    full_document = change.get("fullDocument") or {}
    if "id_proposta" in full_document:
        return str(full_document["id_proposta"]), False

    before = change.get("fullDocumentBeforeChange") or {}
    if "id_proposta" in before:
        return str(before["id_proposta"]), False

    document_key = change.get("documentKey") or {}
    if "_id" in document_key:
        return str(document_key["_id"]), True

    return str(change["_id"]), True


def construir_evento(change: dict) -> EventoAlteracao:
    operation_type = change["operationType"]
    id_proposta, chave_derivada_de_fallback = _extrair_id_proposta(change)
    cluster_time = change.get("clusterTime")

    envelope: dict[str, Any] = {
        "id_proposta": id_proposta,
        "operation_type": operation_type,
        "cluster_time": cluster_time,
        "cluster_time_epoch": cluster_time.time if cluster_time is not None else None,
        "resume_token": change["_id"],
        "document_key": change.get("documentKey"),
        "full_document": change.get("fullDocument"),
        "full_document_before_change": change.get("fullDocumentBeforeChange"),
        "update_description": change.get("updateDescription"),
        "chave_derivada_de_fallback": chave_derivada_de_fallback,
    }

    return EventoAlteracao(
        id_proposta=id_proposta,
        operation_type=operation_type,
        resume_token=change["_id"],
        envelope=envelope,
    )
