from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class DadosConsignado:
    matricula: str
    orgao_convenio: str
    prazo_meses: int
    taxa_juros: float
    margem_consignavel: float

    def to_document(self) -> dict:
        return {
            "matricula": self.matricula,
            "orgao_convenio": self.orgao_convenio,
            "prazo_meses": self.prazo_meses,
            "taxa_juros": self.taxa_juros,
            "margem_consignavel": self.margem_consignavel,
        }
