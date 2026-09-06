import json
import shutil
from datetime import datetime, timezone
from pathlib import Path

import pytest
from pyspark.sql import Row, SparkSession
from pyspark.sql.types import (
    BooleanType,
    DateType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

import silver_job

DIRETORIO_BASE = Path("/tmp/teste_silver_job")

ESQUEMA_BRONZE = StructType(
    [
        StructField("kafka_key", StringType(), True),
        StructField("kafka_topic", StringType(), True),
        StructField("kafka_partition", IntegerType(), True),
        StructField("kafka_offset", LongType(), True),
        StructField("kafka_timestamp", TimestampType(), True),
        StructField("envelope_bruto", StringType(), True),
        StructField("id_proposta", StringType(), True),
        StructField("operation_type", StringType(), True),
        StructField("cluster_time_epoch", LongType(), True),
        StructField("resume_token_data", StringType(), True),
        StructField("chave_derivada_de_fallback", BooleanType(), True),
        StructField("document_key_json", StringType(), True),
        StructField("full_document_json", StringType(), True),
        StructField("full_document_before_change_json", StringType(), True),
        StructField("update_description_json", StringType(), True),
        StructField("data_evento", DateType(), True),
        StructField("data_ingestao", TimestampType(), True),
    ]
)


@pytest.fixture(scope="module")
def spark():
    sessao = SparkSession.builder.appName("teste-silver-job").master("local[2]").getOrCreate()
    sessao.sparkContext.setLogLevel("ERROR")
    yield sessao
    sessao.stop()


@pytest.fixture
def config(spark, tmp_path_factory):
    raiz = DIRETORIO_BASE / str(id(tmp_path_factory))
    if raiz.exists():
        shutil.rmtree(raiz)
    raiz.mkdir(parents=True)
    yield silver_job.ConfigSilver(
        bronze_path=str(raiz / "bronze"),
        historico_path=str(raiz / "historico"),
        estado_atual_path=str(raiz / "estado_atual"),
        checkpoint_path=str(raiz / "checkpoint"),
        watermark_atraso="2 minutes",
        trigger_intervalo="0 seconds",
        shuffle_partitions="2",
    )
    shutil.rmtree(raiz, ignore_errors=True)


def _linha_bronze(
    id_proposta: str,
    operation_type: str,
    cluster_time_epoch: int,
    resume_token_data: str,
    full_document: dict | None = None,
    full_document_before_change: dict | None = None,
    update_description: dict | None = None,
) -> Row:
    agora = datetime.now(timezone.utc)
    return Row(
        kafka_key=id_proposta,
        kafka_topic="propostas.cdc",
        kafka_partition=0,
        kafka_offset=0,
        kafka_timestamp=agora,
        envelope_bruto="{}",
        id_proposta=id_proposta,
        operation_type=operation_type,
        cluster_time_epoch=cluster_time_epoch,
        resume_token_data=resume_token_data,
        chave_derivada_de_fallback=False,
        document_key_json=None,
        full_document_json=json.dumps(full_document) if full_document is not None else None,
        full_document_before_change_json=(
            json.dumps(full_document_before_change) if full_document_before_change is not None else None
        ),
        update_description_json=json.dumps(update_description) if update_description is not None else None,
        data_evento=agora.date(),
        data_ingestao=agora,
    )


def _escrever_no_bronze(spark: SparkSession, config: silver_job.ConfigSilver, linhas: list[Row]) -> None:
    spark.createDataFrame(linhas, ESQUEMA_BRONZE).write.format("delta").mode("append").save(config.bronze_path)


def _ler_historico(spark: SparkSession, config: silver_job.ConfigSilver):
    return spark.read.format("delta").load(config.historico_path).orderBy("valido_de")


def _ler_estado_atual(spark: SparkSession, config: silver_job.ConfigSilver):
    return spark.read.format("delta").load(config.estado_atual_path)


def test_transicao_de_status_cria_intervalo_e_fecha_o_anterior(spark, config):
    _escrever_no_bronze(
        spark,
        config,
        [
            _linha_bronze(
                "prop-1", "insert", 1000, "token-1",
                full_document={"id_proposta": "prop-1", "tipo_produto": "cartao", "status": "em_analise"},
            )
        ],
    )
    query = silver_job.construir_query(spark, config)
    query.processAllAvailable()

    _escrever_no_bronze(
        spark,
        config,
        [
            _linha_bronze(
                "prop-1", "update", 2000, "token-2",
                full_document={"id_proposta": "prop-1", "tipo_produto": "cartao", "status": "aprovada"},
                update_description={"updatedFields": {"status": "aprovada"}},
            )
        ],
    )
    query.processAllAvailable()
    query.stop()

    historico = _ler_historico(spark, config).collect()
    assert len(historico) == 2
    assert historico[0]["status"] == "em_analise"
    assert historico[0]["valido_ate"] is not None
    assert historico[1]["status"] == "aprovada"
    assert historico[1]["valido_ate"] is None

    estado = _ler_estado_atual(spark, config).collect()
    assert len(estado) == 1
    assert estado[0]["status"] == "aprovada"


def test_correcao_sem_mudanca_de_status_nao_cria_linha_no_historico(spark, config):
    _escrever_no_bronze(
        spark,
        config,
        [
            _linha_bronze(
                "prop-2", "insert", 1000, "token-1",
                full_document={
                    "id_proposta": "prop-2", "tipo_produto": "cartao",
                    "status": "em_analise", "valor_solicitado": 100.0,
                },
            )
        ],
    )
    query = silver_job.construir_query(spark, config)
    query.processAllAvailable()

    _escrever_no_bronze(
        spark,
        config,
        [
            _linha_bronze(
                "prop-2", "update", 2000, "token-2",
                full_document={
                    "id_proposta": "prop-2", "tipo_produto": "cartao",
                    "status": "em_analise", "valor_solicitado": 250.0,
                },
                update_description={"updatedFields": {"valor_solicitado": 250.0}},
            )
        ],
    )
    query.processAllAvailable()
    query.stop()

    historico = _ler_historico(spark, config).collect()
    assert len(historico) == 1
    assert historico[0]["valido_ate"] is None

    estado = _ler_estado_atual(spark, config).collect()
    assert estado[0]["valor_solicitado"] == 250.0


def test_delete_remove_do_estado_atual_e_fecha_historico_como_deletada(spark, config):
    _escrever_no_bronze(
        spark,
        config,
        [
            _linha_bronze(
                "prop-3", "insert", 1000, "token-1",
                full_document={"id_proposta": "prop-3", "tipo_produto": "fgts", "status": "em_analise"},
            )
        ],
    )
    query = silver_job.construir_query(spark, config)
    query.processAllAvailable()

    _escrever_no_bronze(
        spark,
        config,
        [
            _linha_bronze(
                "prop-3", "delete", 2000, "token-2",
                full_document_before_change={"id_proposta": "prop-3", "tipo_produto": "fgts", "status": "em_analise"},
            )
        ],
    )
    query.processAllAvailable()
    query.stop()

    estado = _ler_estado_atual(spark, config).filter("id_proposta = 'prop-3'").collect()
    assert len(estado) == 0

    historico = _ler_historico(spark, config).collect()
    assert len(historico) == 2
    assert historico[1]["status"] == "deletada"
    assert historico[1]["valido_ate"] is None


def test_duplicata_exata_dentro_do_watermark_e_descartada(spark, config):
    linha = _linha_bronze(
        "prop-4", "insert", 1000, "token-duplicado",
        full_document={"id_proposta": "prop-4", "tipo_produto": "cartao", "status": "em_analise"},
    )
    _escrever_no_bronze(spark, config, [linha])
    query = silver_job.construir_query(spark, config)
    query.processAllAvailable()

    _escrever_no_bronze(spark, config, [linha])
    query.processAllAvailable()
    query.stop()

    historico = _ler_historico(spark, config).filter("id_proposta = 'prop-4'").collect()
    assert len(historico) == 1

    estado = _ler_estado_atual(spark, config).filter("id_proposta = 'prop-4'").collect()
    assert len(estado) == 1


def test_reprocessamento_atrasado_nao_regride_o_estado_atual(spark, config):
    original = _linha_bronze(
        "prop-5", "update", 5000, "token-original",
        full_document={"id_proposta": "prop-5", "tipo_produto": "cartao", "status": "aprovada"},
        update_description={"updatedFields": {"status": "aprovada"}},
    )
    _escrever_no_bronze(spark, config, [original])
    query = silver_job.construir_query(spark, config)
    query.processAllAvailable()

    mais_novo = _linha_bronze(
        "prop-5", "update", 9000, "token-mais-novo",
        full_document={"id_proposta": "prop-5", "tipo_produto": "cartao", "status": "paga"},
        update_description={"updatedFields": {"status": "paga"}},
    )
    _escrever_no_bronze(spark, config, [mais_novo])
    query.processAllAvailable()

    reprocessamento_atrasado_alem_do_watermark = _linha_bronze(
        "prop-5", "update", 5000, "token-original-reenviado",
        full_document={"id_proposta": "prop-5", "tipo_produto": "cartao", "status": "aprovada"},
        update_description={"updatedFields": {"status": "aprovada"}},
    )
    _escrever_no_bronze(spark, config, [reprocessamento_atrasado_alem_do_watermark])
    query.processAllAvailable()
    query.stop()

    estado = _ler_estado_atual(spark, config).filter("id_proposta = 'prop-5'").collect()
    assert len(estado) == 1
    assert estado[0]["status"] == "paga"
