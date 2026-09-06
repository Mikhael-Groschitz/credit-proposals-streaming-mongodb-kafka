from __future__ import annotations

import random
import uuid

from faker import Faker

from .models.cartao import DadosCartao
from .models.comum import DadosComuns, agora_utc
from .models.consignado import DadosConsignado
from .models.fgts import DadosFgts

_faker = Faker("pt_BR")

_ORGAOS_CONVENIO = ("INSS", "Prefeitura Municipal", "Governo do Estado", "Forcas Armadas")
_BANDEIRAS = ("Visa", "Mastercard", "Elo", "American Express")
_BANCOS_PARCEIROS = ("Banco A", "Banco B", "Banco C", "Banco D")
_PRAZOS_MESES = (12, 24, 36, 48, 60, 72, 84)


def _novo_id_proposta() -> str:
    return str(uuid.uuid4())


def _documento_comum(tipo_produto: str, valor_solicitado: float) -> DadosComuns:
    agora = agora_utc()
    return DadosComuns(
        id_proposta=_novo_id_proposta(),
        tipo_produto=tipo_produto,
        cpf_cliente=_faker.cpf(),
        nome_cliente=_faker.name(),
        valor_solicitado=valor_solicitado,
        status="em_analise",
        data_criacao=agora,
        data_atualizacao=agora,
    )


def gerar_proposta_consignado() -> dict:
    valor = round(random.uniform(1_000, 50_000), 2)
    comuns = _documento_comum("consignado", valor)
    especificos = DadosConsignado(
        matricula=str(random.randint(100_000, 999_999)),
        orgao_convenio=random.choice(_ORGAOS_CONVENIO),
        prazo_meses=random.choice(_PRAZOS_MESES),
        taxa_juros=round(random.uniform(1.2, 2.5), 2),
        margem_consignavel=round(random.uniform(200, 3_000), 2),
    )
    return {**comuns.to_document(), **especificos.to_document()}


def gerar_proposta_cartao() -> dict:
    valor = round(random.uniform(500, 15_000), 2)
    comuns = _documento_comum("cartao", valor)
    especificos = DadosCartao(
        limite_solicitado=round(random.uniform(500, 20_000), 2),
        bandeira=random.choice(_BANDEIRAS),
        score_credito=random.randint(300, 1_000),
    )
    return {**comuns.to_document(), **especificos.to_document()}


def gerar_proposta_fgts() -> dict:
    saldo = round(random.uniform(1_000, 30_000), 2)
    valor_solicitado = round(saldo * random.uniform(0.5, 0.9), 2)
    comuns = _documento_comum("fgts", valor_solicitado)
    especificos = DadosFgts(
        saldo_fgts_disponivel=saldo,
        percentual_antecipado=round(random.uniform(0.5, 0.95), 2),
        banco_parceiro=random.choice(_BANCOS_PARCEIROS),
    )
    return {**comuns.to_document(), **especificos.to_document()}


_GERADORES_POR_TIPO = {
    "consignado": gerar_proposta_consignado,
    "cartao": gerar_proposta_cartao,
    "fgts": gerar_proposta_fgts,
}


def gerar_proposta(tipo_produto: str) -> dict:
    try:
        gerador = _GERADORES_POR_TIPO[tipo_produto]
    except KeyError as exc:
        raise ValueError(f"tipo de produto desconhecido: {tipo_produto!r}") from exc
    return gerador()


def sortear_tipo_produto(pesos: dict[str, float]) -> str:
    tipos = list(pesos.keys())
    pesos_valores = list(pesos.values())
    return random.choices(tipos, weights=pesos_valores, k=1)[0]


def novo_nome_cliente() -> str:
    return _faker.name()


def novo_cpf_cliente() -> str:
    return _faker.cpf()


def novo_valor_solicitado(minimo: float = 500.0, maximo: float = 50_000.0) -> float:
    return round(random.uniform(minimo, maximo), 2)
