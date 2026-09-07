from __future__ import annotations

import os
import time
from dataclasses import dataclass

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import (
    BooleanType,
    DoubleType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from transformacoes import (
    eh_transicao_de_status,
    extrair_campos_especificos,
    mesclar_full_document_com_update_description,
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

ESQUEMA_ESTADO_ATUAL = StructType(
    [
        StructField("id_proposta", StringType(), True),
        StructField("tipo_produto", StringType(), True),
        StructField("cpf_cliente", StringType(), True),
        StructField("nome_cliente", StringType(), True),
        StructField("valor_solicitado", DoubleType(), True),
        StructField("status", StringType(), True),
        StructField("data_criacao", TimestampType(), True),
        StructField("data_atualizacao", TimestampType(), True),
        StructField("campos_especificos_json", StringType(), True),
        StructField("hora_evento", TimestampType(), True),
        StructField("resume_token_data", StringType(), True),
    ]
)


def _str_env(nome: str, padrao: str) -> str:
    valor = os.environ.get(nome)
    return valor if valor else padrao


@dataclass(frozen=True)
class ConfigSilver:
    bronze_path: str
    historico_path: str
    estado_atual_path: str
    checkpoint_path: str
    watermark_atraso: str
    trigger_intervalo: str
    shuffle_partitions: str

    @classmethod
    def from_env(cls) -> "ConfigSilver":
        return cls(
            bronze_path=_str_env("BRONZE_PATH", "s3a://bronze/propostas"),
            historico_path=_str_env("SILVER_HISTORICO_PATH", "s3a://silver/historico"),
            estado_atual_path=_str_env("SILVER_ESTADO_ATUAL_PATH", "s3a://silver/estado_atual"),
            checkpoint_path=_str_env("SILVER_CHECKPOINT_PATH", "s3a://silver/_checkpoints/propostas"),
            watermark_atraso=_str_env("SILVER_WATERMARK_ATRASO", "10 minutes"),
            trigger_intervalo=_str_env("SILVER_TRIGGER_INTERVALO", "10 seconds"),
            shuffle_partitions=_str_env("SILVER_SHUFFLE_PARTITIONS", "6"),
        )


def aguardar_bronze_disponivel(
    spark: SparkSession, bronze_path: str, tentativas_max: int = 30, intervalo_segundos: int = 2
) -> None:
    for _ in range(tentativas_max):
        try:
            spark.read.format("delta").load(bronze_path).limit(0).count()
            return
        except Exception:
            time.sleep(intervalo_segundos)
    raise RuntimeError(f"tabela bronze em {bronze_path!r} nao ficou disponivel a tempo")


def garantir_tabelas(spark: SparkSession, config: ConfigSilver) -> None:
    spark.createDataFrame([], ESQUEMA_HISTORICO).write.format("delta").mode("ignore").save(
        config.historico_path
    )
    spark.createDataFrame([], ESQUEMA_ESTADO_ATUAL).write.format("delta").mode("ignore").save(
        config.estado_atual_path
    )


def preparar_lote(micro_lote: DataFrame) -> DataFrame:
    eh_transicao_udf = F.udf(eh_transicao_de_status, BooleanType())
    campos_especificos_udf = F.udf(extrair_campos_especificos, StringType())
    mesclar_update_description_udf = F.udf(mesclar_full_document_com_update_description, StringType())

    documento_fonte_bruto = F.coalesce(F.col("full_document_json"), F.col("full_document_before_change_json"))
    documento_fonte = mesclar_update_description_udf(documento_fonte_bruto, F.col("update_description_json"))

    return (
        micro_lote.withColumn("documento_fonte_json", documento_fonte)
        .withColumn(
            "status",
            F.when(F.col("operation_type") == "delete", F.lit("deletada")).otherwise(
                F.get_json_object(F.col("documento_fonte_json"), "$.status")
            ),
        )
        .withColumn("tipo_produto", F.get_json_object(F.col("documento_fonte_json"), "$.tipo_produto"))
        .withColumn("cpf_cliente", F.get_json_object(F.col("documento_fonte_json"), "$.cpf_cliente"))
        .withColumn("nome_cliente", F.get_json_object(F.col("documento_fonte_json"), "$.nome_cliente"))
        .withColumn(
            "valor_solicitado",
            F.get_json_object(F.col("documento_fonte_json"), "$.valor_solicitado").cast("double"),
        )
        .withColumn(
            "data_criacao",
            F.to_timestamp(F.get_json_object(F.col("documento_fonte_json"), "$.data_criacao.$date")),
        )
        .withColumn(
            "data_atualizacao",
            F.to_timestamp(F.get_json_object(F.col("documento_fonte_json"), "$.data_atualizacao.$date")),
        )
        .withColumn("campos_especificos_json", campos_especificos_udf(F.col("documento_fonte_json")))
        .withColumn("eh_transicao", eh_transicao_udf(F.col("operation_type"), F.col("update_description_json")))
    )


def escrever_historico(micro_lote: DataFrame, caminho_historico: str) -> None:
    transicoes = micro_lote.filter(F.col("eh_transicao"))
    if transicoes.isEmpty():
        return

    janela_lote = Window.partitionBy("id_proposta").orderBy("hora_evento", "resume_token_data")
    transicoes_com_prox = transicoes.withColumn(
        "valido_ate", F.lead("hora_evento").over(janela_lote)
    ).withColumn("linha_num", F.row_number().over(janela_lote))

    primeira_por_proposta = transicoes_com_prox.filter("linha_num = 1").select(
        "id_proposta", F.col("hora_evento").alias("novo_valido_de")
    )
    spark_do_lote = micro_lote.sparkSession
    primeira_por_proposta.createOrReplaceTempView("primeira_transicao_do_lote")

    spark_do_lote.sql(
        f"""
        MERGE INTO delta.`{caminho_historico}` AS alvo
        USING primeira_transicao_do_lote AS novo
        ON alvo.id_proposta = novo.id_proposta AND alvo.valido_ate IS NULL
        WHEN MATCHED THEN UPDATE SET alvo.valido_ate = novo.novo_valido_de
        """
    )

    novas_linhas = transicoes_com_prox.select(
        "id_proposta",
        "tipo_produto",
        "status",
        F.col("hora_evento").alias("valido_de"),
        "valido_ate",
        "resume_token_data",
    )
    novas_linhas.write.format("delta").mode("append").save(caminho_historico)


def escrever_estado_atual(micro_lote: DataFrame, caminho_estado_atual: str) -> None:
    if micro_lote.isEmpty():
        return

    janela = Window.partitionBy("id_proposta").orderBy(
        F.col("hora_evento").desc(), F.col("resume_token_data").desc()
    )
    ultimo_por_proposta = (
        micro_lote.withColumn("linha_num", F.row_number().over(janela))
        .filter("linha_num = 1")
        .drop("linha_num")
    )
    spark_do_lote = micro_lote.sparkSession
    ultimo_por_proposta.createOrReplaceTempView("ultimo_evento_do_lote")

    spark_do_lote.sql(
        f"""
        MERGE INTO delta.`{caminho_estado_atual}` AS alvo
        USING ultimo_evento_do_lote AS novo
        ON alvo.id_proposta = novo.id_proposta
        WHEN MATCHED AND novo.operation_type = 'delete' THEN DELETE
        WHEN MATCHED AND novo.operation_type != 'delete' AND novo.hora_evento > alvo.hora_evento THEN UPDATE SET
            alvo.tipo_produto = novo.tipo_produto,
            alvo.cpf_cliente = novo.cpf_cliente,
            alvo.nome_cliente = novo.nome_cliente,
            alvo.valor_solicitado = novo.valor_solicitado,
            alvo.status = novo.status,
            alvo.data_criacao = novo.data_criacao,
            alvo.data_atualizacao = novo.data_atualizacao,
            alvo.campos_especificos_json = novo.campos_especificos_json,
            alvo.hora_evento = novo.hora_evento,
            alvo.resume_token_data = novo.resume_token_data
        WHEN NOT MATCHED AND novo.operation_type != 'delete' THEN INSERT (
            id_proposta, tipo_produto, cpf_cliente, nome_cliente, valor_solicitado, status,
            data_criacao, data_atualizacao, campos_especificos_json, hora_evento, resume_token_data
        ) VALUES (
            novo.id_proposta, novo.tipo_produto, novo.cpf_cliente, novo.nome_cliente, novo.valor_solicitado, novo.status,
            novo.data_criacao, novo.data_atualizacao, novo.campos_especificos_json, novo.hora_evento, novo.resume_token_data
        )
        """
    )


def processar_lote(config: ConfigSilver, micro_lote_bruto: DataFrame, id_lote: int) -> None:
    if micro_lote_bruto.isEmpty():
        return

    micro_lote = preparar_lote(micro_lote_bruto).cache()
    try:
        escrever_historico(micro_lote, config.historico_path)
        escrever_estado_atual(micro_lote, config.estado_atual_path)
    finally:
        micro_lote.unpersist()


def construir_query(spark: SparkSession, config: ConfigSilver):
    garantir_tabelas(spark, config)

    leitura = spark.readStream.format("delta").load(config.bronze_path)

    deduplicado = (
        leitura.withColumn("hora_evento", F.to_timestamp(F.from_unixtime(F.col("cluster_time_epoch"))))
        .withWatermark("hora_evento", config.watermark_atraso)
        .dropDuplicatesWithinWatermark(["resume_token_data"])
    )

    return (
        deduplicado.writeStream.foreachBatch(
            lambda lote, id_lote: processar_lote(config, lote, id_lote)
        )
        .option("checkpointLocation", config.checkpoint_path)
        .trigger(processingTime=config.trigger_intervalo)
        .start()
    )


def main() -> None:
    config = ConfigSilver.from_env()

    spark = SparkSession.builder.appName("silver-propostas").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    spark.conf.set("spark.sql.shuffle.partitions", config.shuffle_partitions)

    aguardar_bronze_disponivel(spark, config.bronze_path)

    query = construir_query(spark, config)
    query.awaitTermination()


if __name__ == "__main__":
    main()
