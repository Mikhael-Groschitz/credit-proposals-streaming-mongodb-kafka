from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class DadosFgts:
    saldo_fgts_disponivel: float
    percentual_antecipado: float
    banco_parceiro: str

    def to_document(self) -> dict:
        return {
            "saldo_fgts_disponivel": self.saldo_fgts_disponivel,
            "percentual_antecipado": self.percentual_antecipado,
            "banco_parceiro": self.banco_parceiro,
        }
