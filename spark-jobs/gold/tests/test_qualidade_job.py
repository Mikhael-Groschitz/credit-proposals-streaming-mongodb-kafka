import json
import shutil
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest
from pyspark.sql import Row, SparkSession
from pyspark.sql.types import StringType, StructField, StructType, TimestampType

import qualidade_job

DIRETORIO_BASE = Path("/tmp/teste_qualidade_job")

ESQUEMA_BRONZE_MINIMO = StructType(
    [
        StructField("id_proposta", StringType(), True),
        StructField("operation_type", StringType(), True),
        StructField("full_document_json", StringType(), True),
        StructField("full_document_before_change_json", StringType(), True),
    ]
)

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


@pytest.fixture(scope="module")
def spark():
    sessao = SparkSession.builder.appName("teste-qualidade-job").master("local[2]").getOrCreate()
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
    yield qualidade_job.ConfigQualidade(
        bronze_path=str(raiz / "bronze"),
        historico_path=str(raiz / "historico"),
        violacoes_path=str(raiz / "violacoes"),
        checkpoint_bronze_path=str(raiz / "checkpoint_bronze"),
        checkpoint_historico_path=str(raiz / "checkpoint_historico"),
        trigger_intervalo="0 seconds",
        shuffle_partitions="2",
    )
    shutil.rmtree(raiz, ignore_errors=True)


def _linha_bronze(
    id_proposta: str,
    operation_type: str,
    full_document: dict | None = None,
    full_document_before_change: dict | None = None,
) -> Row:
    return Row(
        id_proposta=id_proposta,
        operation_type=operation_type,
        full_document_json=json.dumps(full_document) if full_document is not None else None,
        full_document_before_change_json=(
            json.dumps(full_document_before_change) if full_document_before_change is not None else None
        ),
    )


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


def _escrever_no_bronze(spark: SparkSession, config: qualidade_job.ConfigQualidade, linhas: list[Row]) -> None:
    spark.createDataFrame(linhas, ESQUEMA_BRONZE_MINIMO).write.format("delta").mode("append").save(
        config.bronze_path
    )


def _escrever_no_historico(spark: SparkSession, config: qualidade_job.ConfigQualidade, linhas: list[Row]) -> None:
    spark.createDataFrame(linhas, ESQUEMA_HISTORICO).write.format("delta").mode("append").save(config.historico_path)


def _ler_violacoes(spark: SparkSession, config: qualidade_job.ConfigQualidade):
    return spark.read.format("delta").load(config.violacoes_path)


def test_documento_valido_nao_gera_violacao(spark, config):
    _escrever_no_bronze(
        spark,
        config,
        [
            _linha_bronze(
                "prop-1",
                "insert",
                full_document={"tipo_produto": "cartao", "status": "em_analise", "valor_solicitado": 5_000.0},
            )
        ],
    )
    _escrever_no_historico(
        spark,
        config,
        [_linha_historico("prop-1", "cartao", "em_analise", datetime(2024, 1, 1, tzinfo=timezone.utc))],
    )
    queries = qualidade_job.construir_queries(spark, config)
    for query in queries:
        query.processAllAvailable()
    for query in queries:
        query.stop()

    assert _ler_violacoes(spark, config).filter("id_proposta = 'prop-1'").count() == 0


def test_status_desconhecido_gera_violacao_de_estado_impossivel(spark, config):
    _escrever_no_bronze(
        spark,
        config,
        [
            _linha_bronze(
                "prop-2",
                "insert",
                full_document={"tipo_produto": "cartao", "status": "status_corrompido", "valor_solicitado": 5_000.0},
            )
        ],
    )
    _escrever_no_historico(spark, config, [])
    queries = qualidade_job.construir_queries(spark, config)
    for query in queries:
        query.processAllAvailable()
    for query in queries:
        query.stop()

    violacoes = _ler_violacoes(spark, config).filter("id_proposta = 'prop-2'").collect()
    assert len(violacoes) == 1
    assert violacoes[0]["tipo_violacao"] == "estado_impossivel"


def test_valor_fora_da_faixa_do_produto_gera_violacao(spark, config):
    _escrever_no_bronze(
        spark,
        config,
        [
            _linha_bronze(
                "prop-3",
                "insert",
                full_document={"tipo_produto": "cartao", "status": "em_analise", "valor_solicitado": 999_999.0},
            )
        ],
    )
    _escrever_no_historico(spark, config, [])
    queries = qualidade_job.construir_queries(spark, config)
    for query in queries:
        query.processAllAvailable()
    for query in queries:
        query.stop()

    violacoes = _ler_violacoes(spark, config).filter("id_proposta = 'prop-3'").collect()
    assert len(violacoes) == 1
    assert violacoes[0]["tipo_violacao"] == "valor_fora_de_faixa"


def test_delete_nao_e_verificado(spark, config):
    _escrever_no_bronze(
        spark,
        config,
        [
            _linha_bronze(
                "prop-4",
                "delete",
                full_document_before_change={
                    "tipo_produto": "cartao",
                    "status": "status_corrompido",
                    "valor_solicitado": -1.0,
                },
            )
        ],
    )
    _escrever_no_historico(spark, config, [])
    queries = qualidade_job.construir_queries(spark, config)
    for query in queries:
        query.processAllAvailable()
    for query in queries:
        query.stop()

    assert _ler_violacoes(spark, config).filter("id_proposta = 'prop-4'").count() == 0


def test_sequencia_de_transicoes_validas_nao_gera_violacao(spark, config):
    inicio = datetime(2024, 1, 1, tzinfo=timezone.utc)
    meio = inicio + timedelta(hours=1)

    _escrever_no_historico(
        spark,
        config,
        [
            _linha_historico("prop-5", "consignado", "em_analise", inicio, valido_ate=meio),
            _linha_historico("prop-5", "consignado", "aprovada", meio),
        ],
    )
    _escrever_no_bronze(spark, config, [])
    queries = qualidade_job.construir_queries(spark, config)
    for query in queries:
        query.processAllAvailable()
    for query in queries:
        query.stop()

    assert _ler_violacoes(spark, config).filter("id_proposta = 'prop-5'").count() == 0


def test_transicao_partindo_de_estado_terminal_gera_violacao(spark, config):
    inicio = datetime(2024, 1, 1, tzinfo=timezone.utc)
    depois_do_terminal = inicio + timedelta(hours=1)

    _escrever_no_historico(
        spark,
        config,
        [_linha_historico("prop-6", "consignado", "recusada", inicio, valido_ate=depois_do_terminal)],
    )
    _escrever_no_bronze(spark, config, [])
    queries = qualidade_job.construir_queries(spark, config)
    for query in queries:
        query.processAllAvailable()

    _escrever_no_historico(
        spark,
        config,
        [_linha_historico("prop-6", "consignado", "aprovada", depois_do_terminal)],
    )
    for query in queries:
        query.processAllAvailable()
    for query in queries:
        query.stop()

    violacoes = _ler_violacoes(spark, config).filter("id_proposta = 'prop-6'").collect()
    assert len(violacoes) == 1
    assert violacoes[0]["tipo_violacao"] == "transicao_invalida"
    assert violacoes[0]["detalhe"] == "recusada -> aprovada"


def test_delete_de_qualquer_estado_nao_gera_violacao_de_transicao(spark, config):
    inicio = datetime(2024, 1, 1, tzinfo=timezone.utc)
    momento_delete = inicio + timedelta(hours=1)

    _escrever_no_historico(
        spark,
        config,
        [
            _linha_historico("prop-7", "cartao", "paga", inicio, valido_ate=momento_delete),
            _linha_historico("prop-7", "cartao", "deletada", momento_delete),
        ],
    )
    _escrever_no_bronze(spark, config, [])
    queries = qualidade_job.construir_queries(spark, config)
    for query in queries:
        query.processAllAvailable()
    for query in queries:
        query.stop()

    assert _ler_violacoes(spark, config).filter("id_proposta = 'prop-7'").count() == 0


def test_transicoes_no_mesmo_segundo_usam_resume_token_como_desempate(spark, config):
    mesmo_segundo = datetime(2024, 1, 1, 10, 0, 0, tzinfo=timezone.utc)

    _escrever_no_historico(
        spark,
        config,
        [
            _linha_historico(
                "prop-8", "cartao", "em_analise", mesmo_segundo, valido_ate=mesmo_segundo, resume_token_data="tok-1"
            ),
            _linha_historico(
                "prop-8", "cartao", "aprovada", mesmo_segundo, valido_ate=mesmo_segundo, resume_token_data="tok-2"
            ),
            _linha_historico("prop-8", "cartao", "paga", mesmo_segundo, resume_token_data="tok-3"),
        ],
    )
    _escrever_no_bronze(spark, config, [])
    queries = qualidade_job.construir_queries(spark, config)
    for query in queries:
        query.processAllAvailable()
    for query in queries:
        query.stop()

    assert _ler_violacoes(spark, config).filter("id_proposta = 'prop-8'").count() == 0
