from __future__ import annotations

TRANSICOES_VALIDAS: frozenset[tuple[str, str]] = frozenset(
    {
        ("em_analise", "pendente_documentacao"),
        ("em_analise", "aprovada"),
        ("em_analise", "recusada"),
        ("em_analise", "cancelada"),
        ("pendente_documentacao", "em_analise"),
        ("pendente_documentacao", "aprovada"),
        ("pendente_documentacao", "recusada"),
        ("pendente_documentacao", "cancelada"),
        ("aprovada", "paga"),
        ("aprovada", "cancelada"),
    }
)

STATUS_CONHECIDOS = frozenset(
    {
        "em_analise",
        "pendente_documentacao",
        "aprovada",
        "recusada",
        "paga",
        "cancelada",
        "deletada",
    }
)

FAIXA_VALOR_POR_PRODUTO: dict[str, tuple[float, float]] = {
    "consignado": (1_000.0, 50_000.0),
    "cartao": (500.0, 15_000.0),
    "fgts": (1_000.0, 27_000.0),
}


def estado_e_impossivel(status: str | None) -> bool:
    if status is None:
        return True
    return status not in STATUS_CONHECIDOS


def valor_fora_de_faixa(valor: float | None, tipo_produto: str | None) -> bool:
    if valor is None or tipo_produto is None:
        return True
    faixa = FAIXA_VALOR_POR_PRODUTO.get(tipo_produto)
    if faixa is None:
        return True
    minimo, maximo = faixa
    return valor < minimo or valor > maximo


def transicao_e_valida(status_anterior: str | None, status_novo: str) -> bool:
    if status_anterior is None:
        return True
    if status_anterior == status_novo:
        return True
    if status_novo == "deletada":
        return True
    return (status_anterior, status_novo) in TRANSICOES_VALIDAS
