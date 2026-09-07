import json
import shutil
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest
from pyspark.sql import Row, SparkSession
from pyspark.sql.types import LongType, StringType, StructField, StructType, TimestampType

import gold_job

DIRETORIO_BASE = Path("/tmp/teste_gold_job")

ESQUEMA_HISTORICO = StructType(
    [
        StructField("id_proposta", StringType(), True),
        StructField("tipo_produto", StringType(), True),
        StructField("status", StringType(), True),
        StructField("valido_de", TimestampType(), True),
        StructField("valido_ate", TimestampType(), True),
        StructField("resume_token_data", StringType(), True),
    ]
)

ESQUEMA_BRONZE_MINIMO = StructType(
    [
        StructField("id_proposta", StringType(), True),
        StructField("operation_type", StringType(), True),
        StructField("cluster_time_epoch", LongType(), True),
        StructField("full_document_json", StringType(), True),
    ]
)


@pytest.fixture(scope="module")
def spark():
    sessao = SparkSession.builder.appName("teste-gold-job").master("local[2]").getOrCreate()
    sessao.sparkContext.setLogLevel("ERROR")
    sessao.conf.set("spark.sql.session.timeZone", "UTC")
    yield sessao
    sessao.stop()


@pytest.fixture
def config(spark, tmp_path_factory):
    raiz = DIRETORIO_BASE / str(id(tmp_path_factory))
    if raiz.exists():
        shutil.rmtree(raiz)
    raiz.mkdir(parents=True)
    yield gold_job.ConfigGold(
        bronze_path=str(raiz / "bronze"),
        historico_path=str(raiz / "historico"),
        funil_path=str(raiz / "funil"),
        volume_valor_path=str(raiz / "volume_valor"),
        tempo_decisao_path=str(raiz / "tempo_decisao"),
        contagem_atual_path=str(raiz / "contagem_atual"),
        checkpoint_funil_path=str(raiz / "checkpoint_funil"),
        checkpoint_volume_valor_path=str(raiz / "checkpoint_volume_valor"),
        checkpoint_metricas_path=str(raiz / "checkpoint_metricas"),
        watermark_atraso="30 seconds",
        janela_funil="1 minute",
        trigger_intervalo="0 seconds",
        shuffle_partitions="2",
    )
    shutil.rmtree(raiz, ignore_errors=True)


def _linha_bronze(
    id_proposta: str, operation_type: str, cluster_time_epoch: int, full_document: dict | None = None
) -> Row:
    return Row(
        id_proposta=id_proposta,
        operation_type=operation_type,
        cluster_time_epoch=cluster_time_epoch,
        full_document_json=json.dumps(full_document) if full_document is not None else None,
    )


def _escrever_no_bronze(spark: SparkSession, config: gold_job.ConfigGold, linhas: list[Row]) -> None:
    spark.createDataFrame(linhas, ESQUEMA_BRONZE_MINIMO).write.format("delta").mode("append").save(
        config.bronze_path
    )


def _ler_volume_valor(spark: SparkSession, config: gold_job.ConfigGold):
    return spark.read.format("delta").load(config.volume_valor_path)


def _linha_historico(
    id_proposta: str,
    tipo_produto: str,
    status: str,
    valido_de: datetime,
    valido_ate: datetime | None = None,
    resume_token_data: str = "tok",
) -> Row:
    return Row(
        id_proposta=id_proposta,
        tipo_produto=tipo_produto,
        status=status,
        valido_de=valido_de,
        valido_ate=valido_ate,
        resume_token_data=resume_token_data,
    )


def _escrever_no_historico(spark: SparkSession, config: gold_job.ConfigGold, linhas: list[Row]) -> None:
    spark.createDataFrame(linhas, ESQUEMA_HISTORICO).write.format("delta").mode("append").save(config.historico_path)


def _ler_funil(spark: SparkSession, config: gold_job.ConfigGold):
    return spark.read.format("delta").load(config.funil_path)


def _ler_tempo_decisao(spark: SparkSession, config: gold_job.ConfigGold):
    return spark.read.format("delta").load(config.tempo_decisao_path)


def _ler_contagem_atual(spark: SparkSession, config: gold_job.ConfigGold):
    return spark.read.format("delta").load(config.contagem_atual_path)


def test_agregacao_de_janela_incrementa_quantidade_no_mesmo_grupo(spark, config):
    base = datetime(2024, 1, 1, 10, 0, 10, tzinfo=timezone.utc)

    _escrever_no_historico(
        spark, config, [_linha_historico("prop-1", "cartao", "em_analise", base)]
    )
    _escrever_no_bronze(spark, config, [])
    queries = gold_job.construir_queries(spark, config)
    for query in queries:
        query.processAllAvailable()

    _escrever_no_historico(
        spark, config, [_linha_historico("prop-2", "cartao", "em_analise", base + timedelta(seconds=20))]
    )
    for query in queries:
        query.processAllAvailable()
    for query in queries:
        query.stop()

    funil = (
        _ler_funil(spark, config)
        .filter("tipo_produto = 'cartao' and status = 'em_analise'")
        .collect()
    )
    assert len(funil) == 1
    assert funil[0]["quantidade"] == 2


def test_evento_muito_atrasado_nao_atualiza_janela_ja_expirada(spark, config):
    janela_antiga = datetime(2024, 1, 1, 10, 0, 10, tzinfo=timezone.utc)
    muito_depois = datetime(2024, 1, 1, 11, 0, 0, tzinfo=timezone.utc)

    _escrever_no_historico(
        spark, config, [_linha_historico("prop-3", "fgts", "em_analise", janela_antiga)]
    )
    _escrever_no_bronze(spark, config, [])
    queries = gold_job.construir_queries(spark, config)
    for query in queries:
        query.processAllAvailable()

    _escrever_no_historico(
        spark, config, [_linha_historico("prop-4", "fgts", "em_analise", muito_depois)]
    )
    for query in queries:
        query.processAllAvailable()

    _escrever_no_historico(
        spark,
        config,
        [_linha_historico("prop-5", "fgts", "em_analise", janela_antiga + timedelta(seconds=5))],
    )
    for query in queries:
        query.processAllAvailable()
    for query in queries:
        query.stop()

    funil_fgts = _ler_funil(spark, config).filter("tipo_produto = 'fgts' and status = 'em_analise'").collect()
    janela_antiga_inicio = janela_antiga.replace(second=0, microsecond=0)
    quantidade_janela_antiga = next(
        linha["quantidade"] for linha in funil_fgts if linha["janela_inicio"] == janela_antiga_inicio.replace(tzinfo=None)
    )
    assert quantidade_janela_antiga == 1


def test_tempo_decisao_calcula_intervalo_entre_criacao_e_decisao(spark, config):
    criacao = datetime(2024, 1, 1, 9, 0, 0, tzinfo=timezone.utc)
    decisao = datetime(2024, 1, 1, 9, 30, 0, tzinfo=timezone.utc)

    _escrever_no_historico(
        spark,
        config,
        [
            _linha_historico("prop-6", "consignado", "em_analise", criacao, valido_ate=decisao),
            _linha_historico("prop-6", "consignado", "aprovada", decisao),
        ],
    )
    _escrever_no_bronze(spark, config, [])
    queries = gold_job.construir_queries(spark, config)
    for query in queries:
        query.processAllAvailable()
    for query in queries:
        query.stop()

    tempo_decisao = _ler_tempo_decisao(spark, config).filter("id_proposta = 'prop-6'").collect()
    assert len(tempo_decisao) == 1
    assert tempo_decisao[0]["status_decisao"] == "aprovada"
    assert tempo_decisao[0]["tempo_decisao_segundos"] == 1800


def test_volume_e_valor_somam_apenas_criacoes_do_produto(spark, config):
    momento = int(datetime(2024, 1, 1, 12, 0, 0, tzinfo=timezone.utc).timestamp())

    _escrever_no_historico(spark, config, [])
    _escrever_no_bronze(
        spark,
        config,
        [
            _linha_bronze(
                "prop-9", "insert", momento, full_document={"tipo_produto": "consignado", "valor_solicitado": 10_000.0}
            ),
            _linha_bronze(
                "prop-10", "insert", momento, full_document={"tipo_produto": "consignado", "valor_solicitado": 5_000.0}
            ),
            _linha_bronze(
                "prop-9",
                "update",
                momento + 60,
                full_document={"tipo_produto": "consignado", "valor_solicitado": 10_000.0, "status": "aprovada"},
            ),
        ],
    )
    queries = gold_job.construir_queries(spark, config)
    for query in queries:
        query.processAllAvailable()
    for query in queries:
        query.stop()

    volume_valor = _ler_volume_valor(spark, config).filter("tipo_produto = 'consignado'").collect()
    assert len(volume_valor) == 1
    assert volume_valor[0]["volume"] == 2
    assert volume_valor[0]["valor_total"] == 15_000.0


def test_contagem_atual_reflete_apenas_propostas_ainda_abertas(spark, config):
    aberta = datetime(2024, 1, 1, 8, 0, 0, tzinfo=timezone.utc)
    fechada = datetime(2024, 1, 1, 8, 10, 0, tzinfo=timezone.utc)

    _escrever_no_historico(
        spark,
        config,
        [
            _linha_historico("prop-7", "cartao", "em_analise", aberta),
            _linha_historico("prop-8", "cartao", "em_analise", aberta, valido_ate=fechada),
            _linha_historico("prop-8", "cartao", "aprovada", fechada),
        ],
    )
    _escrever_no_bronze(spark, config, [])
    queries = gold_job.construir_queries(spark, config)
    for query in queries:
        query.processAllAvailable()
    for query in queries:
        query.stop()

    contagem = _ler_contagem_atual(spark, config).filter("tipo_produto = 'cartao'").collect()
    por_status = {linha["status"]: linha["quantidade"] for linha in contagem}
    assert por_status.get("em_analise") == 1
    assert por_status.get("aprovada") == 1
