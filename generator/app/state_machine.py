from __future__ import annotations

import random

TRANSICOES: dict[str, tuple[str, ...]] = {
    "em_analise": ("pendente_documentacao", "aprovada", "recusada", "cancelada"),
    "pendente_documentacao": ("em_analise", "aprovada", "recusada", "cancelada"),
    "aprovada": ("paga", "cancelada"),
    "recusada": (),
    "paga": (),
    "cancelada": (),
}

ESTADOS_TERMINAIS = frozenset(status for status, destinos in TRANSICOES.items() if not destinos)

_PESOS_POR_ORIGEM: dict[str, dict[str, float]] = {
    "em_analise": {
        "pendente_documentacao": 0.25,
        "aprovada": 0.45,
        "recusada": 0.25,
        "cancelada": 0.05,
    },
    "pendente_documentacao": {
        "em_analise": 0.6,
        "aprovada": 0.2,
        "recusada": 0.1,
        "cancelada": 0.1,
    },
    "aprovada": {"paga": 0.85, "cancelada": 0.15},
}


def proximo_status(status_atual: str) -> str | None:
    destinos_possiveis = TRANSICOES.get(status_atual)
    if destinos_possiveis is None:
        raise ValueError(f"status desconhecido: {status_atual!r}")
    if not destinos_possiveis:
        return None
    pesos = _PESOS_POR_ORIGEM[status_atual]
    return random.choices(list(pesos.keys()), weights=list(pesos.values()), k=1)[0]


def eh_terminal(status: str) -> bool:
    if status not in TRANSICOES:
        raise ValueError(f"status desconhecido: {status!r}")
    return status in ESTADOS_TERMINAIS
