from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class DadosCartao:
    limite_solicitado: float
    bandeira: str
    score_credito: int

    def to_document(self) -> dict:
        return {
            "limite_solicitado": self.limite_solicitado,
            "bandeira": self.bandeira,
            "score_credito": self.score_credito,
        }
