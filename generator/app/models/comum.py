from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone


def agora_utc() -> datetime:
    return datetime.now(timezone.utc)


@dataclass(frozen=True)
class DadosComuns:
    id_proposta: str
    tipo_produto: str
    cpf_cliente: str
    nome_cliente: str
    valor_solicitado: float
    status: str
    data_criacao: datetime
    data_atualizacao: datetime

    def to_document(self) -> dict:
        return {
            "id_proposta": self.id_proposta,
            "tipo_produto": self.tipo_produto,
            "cpf_cliente": self.cpf_cliente,
            "nome_cliente": self.nome_cliente,
            "valor_solicitado": self.valor_solicitado,
            "status": self.status,
            "data_criacao": self.data_criacao,
            "data_atualizacao": self.data_atualizacao,
        }
