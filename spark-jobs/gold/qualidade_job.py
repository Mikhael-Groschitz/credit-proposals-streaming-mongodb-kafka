from __future__ import annotations

import os
from dataclasses import dataclass

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import BooleanType, StringType, StructField, StructType, TimestampType
from pyspark.sql.window import Window

from transformacoes import estado_e_impossivel, transicao_e_valida, valor_fora_de_faixa

ESQUEMA_VIOLACOES = StructType(
    [
        StructField("tipo_violacao", StringType(), True),
        StructField("id_proposta", StringType(), True),
        StructField("tipo_produto", StringType(), True),
        StructField("detalhe", StringType(), True),
        StructField("detectado_em", TimestampType(), True),
    ]
)


def _str_env(nome: str, padrao: str) -> str:
    valor = os.environ.get(nome)
    return valor if valor else padrao


@dataclass(frozen=True)
class ConfigQualidade:
    bronze_path: str
    historico_path: str
    violacoes_path: str
    checkpoint_bronze_path: str
    checkpoint_historico_path: str
    trigger_intervalo: str
    shuffle_partitions: str

    @classmethod
    def from_env(cls) -> "ConfigQualidade":
        return cls(
            bronze_path=_str_env("BRONZE_PATH", "s3a://bronze/propostas"),
            historico_path=_str_env("SILVER_HISTORICO_PATH", "s3a://silver/historico"),
            violacoes_path=_str_env("GOLD_VIOLACOES_PATH", "s3a://gold/qualidade_violacoes"),
            checkpoint_bronze_path=_str_env(
                "GOLD_CHECKPOINT_QUALIDADE_BRONZE_PATH", "s3a://gold/_checkpoints/qualidade_bronze"
            ),
            checkpoint_historico_path=_str_env(
                "GOLD_CHECKPOINT_QUALIDADE_HISTORICO_PATH", "s3a://gold/_checkpoints/qualidade_historico"
            ),
            trigger_intervalo=_str_env("GOLD_TRIGGER_INTERVALO", "10 seconds"),
            shuffle_partitions=_str_env("GOLD_SHUFFLE_PARTITIONS", "6"),
        )


def aguardar_tabela_disponivel(
    spark: SparkSession, caminho: str, tentativas_max: int = 30, intervalo_segundos: int = 2
) -> None:
    import time

    for _ in range(tentativas_max):
        try:
            spark.read.format("delta").load(caminho).limit(0).count()
            return
        except Exception:
            time.sleep(intervalo_segundos)
    raise RuntimeError(f"tabela em {caminho!r} nao ficou disponivel a tempo")


def garantir_tabela_violacoes(spark: SparkSession, caminho: str) -> None:
    spark.createDataFrame([], ESQUEMA_VIOLACOES).write.format("delta").mode("ignore").save(caminho)


def checar_estado_e_valor(micro_lote_bronze: DataFrame) -> DataFrame:
    estado_impossivel_udf = F.udf(estado_e_impossivel, BooleanType())
    valor_fora_de_faixa_udf = F.udf(valor_fora_de_faixa, BooleanType())

    documento_fonte = F.coalesce(F.col("full_document_json"), F.col("full_document_before_change_json"))

    preparado = (
        micro_lote_bronze.filter(F.col("operation_type") != "delete")
        .withColumn("tipo_produto_doc", F.get_json_object(documento_fonte, "$.tipo_produto"))
        .withColumn("status_doc", F.get_json_object(documento_fonte, "$.status"))
        .withColumn(
            "valor_solicitado_doc",
            F.get_json_object(documento_fonte, "$.valor_solicitado").cast("double"),
        )
        .withColumn("estado_impossivel", estado_impossivel_udf(F.col("status_doc")))
        .withColumn(
            "valor_fora_de_faixa", valor_fora_de_faixa_udf(F.col("valor_solicitado_doc"), F.col("tipo_produto_doc"))
        )
    )

    violacoes_estado = (
        preparado.filter(F.col("estado_impossivel"))
        .select(
            F.lit("estado_impossivel").alias("tipo_violacao"),
            "id_proposta",
            F.col("tipo_produto_doc").alias("tipo_produto"),
            F.concat(F.lit("status desconhecido: "), F.coalesce(F.col("status_doc"), F.lit("nulo"))).alias(
                "detalhe"
            ),
        )
    )

    violacoes_valor = (
        preparado.filter(F.col("valor_fora_de_faixa"))
        .select(
            F.lit("valor_fora_de_faixa").alias("tipo_violacao"),
            "id_proposta",
            F.col("tipo_produto_doc").alias("tipo_produto"),
            F.concat(
                F.lit("valor_solicitado fora da faixa esperada: "),
                F.coalesce(F.col("valor_solicitado_doc").cast("string"), F.lit("nulo")),
            ).alias("detalhe"),
        )
    )

    return violacoes_estado.unionByName(violacoes_valor).withColumn("detectado_em", F.current_timestamp())


def checar_transicoes(micro_lote_historico: DataFrame, caminho_historico: str) -> DataFrame:
    spark_do_lote = micro_lote_historico.sparkSession
    transicao_e_valida_udf = F.udf(transicao_e_valida, BooleanType())

    ids_do_lote = micro_lote_historico.select("id_proposta").distinct()
    tokens_do_lote = micro_lote_historico.select(
        F.col("resume_token_data").alias("resume_token_data_do_lote")
    ).distinct()

    historico_completo = (
        spark_do_lote.read.format("delta")
        .load(caminho_historico)
        .join(F.broadcast(ids_do_lote), on="id_proposta", how="inner")
    )

    janela_por_proposta = Window.partitionBy("id_proposta").orderBy("valido_de", "resume_token_data")
    com_predecessor = historico_completo.withColumn(
        "status_anterior", F.lag("status").over(janela_por_proposta)
    ).withColumn("transicao_valida", transicao_e_valida_udf(F.col("status_anterior"), F.col("status")))

    return com_predecessor.join(
        F.broadcast(tokens_do_lote),
        com_predecessor["resume_token_data"] == F.col("resume_token_data_do_lote"),
        how="inner",
    ).filter(~F.col("transicao_valida")).select(
        F.lit("transicao_invalida").alias("tipo_violacao"),
        "id_proposta",
        "tipo_produto",
        F.concat(
            F.coalesce(F.col("status_anterior"), F.lit("nulo")), F.lit(" -> "), F.col("status")
        ).alias("detalhe"),
    )


def processar_lote_bronze(config: ConfigQualidade, micro_lote: DataFrame, id_lote: int) -> None:
    if micro_lote.isEmpty():
        return
    violacoes = checar_estado_e_valor(micro_lote)
    if not violacoes.isEmpty():
        violacoes.write.format("delta").mode("append").save(config.violacoes_path)


def processar_lote_historico(config: ConfigQualidade, micro_lote: DataFrame, id_lote: int) -> None:
    if micro_lote.isEmpty():
        return
    violacoes = checar_transicoes(micro_lote, config.historico_path).withColumn(
        "detectado_em", F.current_timestamp()
    )
    if not violacoes.isEmpty():
        violacoes.write.format("delta").mode("append").save(config.violacoes_path)


def construir_queries(spark: SparkSession, config: ConfigQualidade) -> list:
    garantir_tabela_violacoes(spark, config.violacoes_path)

    leitura_bronze = spark.readStream.format("delta").load(config.bronze_path)
    query_bronze = (
        leitura_bronze.writeStream.foreachBatch(lambda lote, id_lote: processar_lote_bronze(config, lote, id_lote))
        .option("checkpointLocation", config.checkpoint_bronze_path)
        .trigger(processingTime=config.trigger_intervalo)
        .start()
    )

    leitura_historico = (
        spark.readStream.format("delta").option("skipChangeCommits", "true").load(config.historico_path)
    )
    query_historico = (
        leitura_historico.writeStream.foreachBatch(
            lambda lote, id_lote: processar_lote_historico(config, lote, id_lote)
        )
        .option("checkpointLocation", config.checkpoint_historico_path)
        .trigger(processingTime=config.trigger_intervalo)
        .start()
    )

    return [query_bronze, query_historico]


def main() -> None:
    config = ConfigQualidade.from_env()

    spark = SparkSession.builder.appName("qualidade-propostas").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    spark.conf.set("spark.sql.shuffle.partitions", config.shuffle_partitions)

    aguardar_tabela_disponivel(spark, config.bronze_path)
    aguardar_tabela_disponivel(spark, config.historico_path)

    construir_queries(spark, config)
    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    main()
