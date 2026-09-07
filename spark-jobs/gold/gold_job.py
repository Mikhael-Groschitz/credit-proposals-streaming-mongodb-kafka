from __future__ import annotations

import os
from dataclasses import dataclass

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, LongType, StringType, StructField, StructType, TimestampType

ESQUEMA_FUNIL = StructType(
    [
        StructField("janela_inicio", TimestampType(), True),
        StructField("janela_fim", TimestampType(), True),
        StructField("tipo_produto", StringType(), True),
        StructField("status", StringType(), True),
        StructField("quantidade", LongType(), True),
    ]
)

ESQUEMA_TEMPO_DECISAO = StructType(
    [
        StructField("id_proposta", StringType(), True),
        StructField("tipo_produto", StringType(), True),
        StructField("status_decisao", StringType(), True),
        StructField("data_criacao", TimestampType(), True),
        StructField("data_decisao", TimestampType(), True),
        StructField("tempo_decisao_segundos", LongType(), True),
    ]
)

ESQUEMA_CONTAGEM_ATUAL = StructType(
    [
        StructField("tipo_produto", StringType(), True),
        StructField("status", StringType(), True),
        StructField("quantidade", LongType(), True),
    ]
)

ESQUEMA_VOLUME_VALOR = StructType(
    [
        StructField("janela_inicio", TimestampType(), True),
        StructField("janela_fim", TimestampType(), True),
        StructField("tipo_produto", StringType(), True),
        StructField("volume", LongType(), True),
        StructField("valor_total", DoubleType(), True),
    ]
)

STATUS_DE_DECISAO = ["aprovada", "recusada"]


def _str_env(nome: str, padrao: str) -> str:
    valor = os.environ.get(nome)
    return valor if valor else padrao


@dataclass(frozen=True)
class ConfigGold:
    bronze_path: str
    historico_path: str
    funil_path: str
    volume_valor_path: str
    tempo_decisao_path: str
    contagem_atual_path: str
    checkpoint_funil_path: str
    checkpoint_volume_valor_path: str
    checkpoint_metricas_path: str
    watermark_atraso: str
    janela_funil: str
    trigger_intervalo: str
    shuffle_partitions: str

    @classmethod
    def from_env(cls) -> "ConfigGold":
        return cls(
            bronze_path=_str_env("BRONZE_PATH", "s3a://bronze/propostas"),
            historico_path=_str_env("SILVER_HISTORICO_PATH", "s3a://silver/historico"),
            funil_path=_str_env("GOLD_FUNIL_PATH", "s3a://gold/funil_status"),
            volume_valor_path=_str_env("GOLD_VOLUME_VALOR_PATH", "s3a://gold/volume_valor_por_produto"),
            tempo_decisao_path=_str_env("GOLD_TEMPO_DECISAO_PATH", "s3a://gold/tempo_decisao"),
            contagem_atual_path=_str_env("GOLD_CONTAGEM_ATUAL_PATH", "s3a://gold/contagem_atual_por_status"),
            checkpoint_funil_path=_str_env("GOLD_CHECKPOINT_FUNIL_PATH", "s3a://gold/_checkpoints/funil"),
            checkpoint_volume_valor_path=_str_env(
                "GOLD_CHECKPOINT_VOLUME_VALOR_PATH", "s3a://gold/_checkpoints/volume_valor"
            ),
            checkpoint_metricas_path=_str_env("GOLD_CHECKPOINT_METRICAS_PATH", "s3a://gold/_checkpoints/metricas"),
            watermark_atraso=_str_env("GOLD_WATERMARK_ATRASO", "10 minutes"),
            janela_funil=_str_env("GOLD_JANELA_FUNIL", "1 day"),
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


def garantir_tabelas_gold(spark: SparkSession, config: ConfigGold) -> None:
    spark.createDataFrame([], ESQUEMA_FUNIL).write.format("delta").mode("ignore").save(config.funil_path)
    spark.createDataFrame([], ESQUEMA_VOLUME_VALOR).write.format("delta").mode("ignore").save(
        config.volume_valor_path
    )
    spark.createDataFrame([], ESQUEMA_TEMPO_DECISAO).write.format("delta").mode("ignore").save(
        config.tempo_decisao_path
    )
    spark.createDataFrame([], ESQUEMA_CONTAGEM_ATUAL).write.format("delta").mode("ignore").save(
        config.contagem_atual_path
    )


def construir_agregacao_funil(leitura_historico: DataFrame, watermark_atraso: str, janela: str) -> DataFrame:
    return (
        leitura_historico.withWatermark("valido_de", watermark_atraso)
        .groupBy(F.window("valido_de", janela).alias("janela"), "tipo_produto", "status")
        .agg(F.count("*").alias("quantidade"))
        .select(
            F.col("janela.start").alias("janela_inicio"),
            F.col("janela.end").alias("janela_fim"),
            "tipo_produto",
            "status",
            "quantidade",
        )
    )


def construir_agregacao_volume_valor(leitura_bronze: DataFrame, watermark_atraso: str, janela: str) -> DataFrame:
    criacoes = (
        leitura_bronze.filter(F.col("operation_type") == "insert")
        .withColumn("hora_evento", F.to_timestamp(F.from_unixtime(F.col("cluster_time_epoch"))))
        .withColumn("tipo_produto", F.get_json_object(F.col("full_document_json"), "$.tipo_produto"))
        .withColumn(
            "valor_solicitado",
            F.get_json_object(F.col("full_document_json"), "$.valor_solicitado").cast("double"),
        )
    )

    return (
        criacoes.withWatermark("hora_evento", watermark_atraso)
        .groupBy(F.window("hora_evento", janela).alias("janela"), "tipo_produto")
        .agg(F.count("*").alias("volume"), F.sum("valor_solicitado").alias("valor_total"))
        .select(
            F.col("janela.start").alias("janela_inicio"),
            F.col("janela.end").alias("janela_fim"),
            "tipo_produto",
            "volume",
            "valor_total",
        )
    )


def escrever_volume_valor(micro_lote: DataFrame, caminho_volume_valor: str) -> None:
    if micro_lote.isEmpty():
        return

    spark_do_lote = micro_lote.sparkSession
    micro_lote.createOrReplaceTempView("agregado_volume_valor_do_lote")

    spark_do_lote.sql(
        f"""
        MERGE INTO delta.`{caminho_volume_valor}` AS alvo
        USING agregado_volume_valor_do_lote AS novo
        ON alvo.janela_inicio = novo.janela_inicio
            AND alvo.janela_fim = novo.janela_fim
            AND alvo.tipo_produto = novo.tipo_produto
        WHEN MATCHED THEN UPDATE SET alvo.volume = novo.volume, alvo.valor_total = novo.valor_total
        WHEN NOT MATCHED THEN INSERT (janela_inicio, janela_fim, tipo_produto, volume, valor_total)
        VALUES (novo.janela_inicio, novo.janela_fim, novo.tipo_produto, novo.volume, novo.valor_total)
        """
    )


def escrever_funil(micro_lote: DataFrame, caminho_funil: str) -> None:
    if micro_lote.isEmpty():
        return

    spark_do_lote = micro_lote.sparkSession
    micro_lote.createOrReplaceTempView("agregado_funil_do_lote")

    spark_do_lote.sql(
        f"""
        MERGE INTO delta.`{caminho_funil}` AS alvo
        USING agregado_funil_do_lote AS novo
        ON alvo.janela_inicio = novo.janela_inicio
            AND alvo.janela_fim = novo.janela_fim
            AND alvo.tipo_produto = novo.tipo_produto
            AND alvo.status = novo.status
        WHEN MATCHED THEN UPDATE SET alvo.quantidade = novo.quantidade
        WHEN NOT MATCHED THEN INSERT (janela_inicio, janela_fim, tipo_produto, status, quantidade)
        VALUES (novo.janela_inicio, novo.janela_fim, novo.tipo_produto, novo.status, novo.quantidade)
        """
    )


def escrever_tempo_decisao(micro_lote: DataFrame, caminho_historico: str, caminho_tempo_decisao: str) -> None:
    decisoes = micro_lote.filter(F.col("status").isin(STATUS_DE_DECISAO))
    if decisoes.isEmpty():
        return

    spark_do_lote = micro_lote.sparkSession
    ids_do_lote = decisoes.select("id_proposta").distinct()

    primeira_entrada = (
        spark_do_lote.read.format("delta")
        .load(caminho_historico)
        .join(F.broadcast(ids_do_lote), on="id_proposta", how="inner")
        .groupBy("id_proposta")
        .agg(F.min("valido_de").alias("data_criacao"))
    )

    tempos = (
        decisoes.select(
            "id_proposta",
            "tipo_produto",
            F.col("status").alias("status_decisao"),
            F.col("valido_de").alias("data_decisao"),
        )
        .join(primeira_entrada, on="id_proposta", how="inner")
        .withColumn(
            "tempo_decisao_segundos",
            F.col("data_decisao").cast("long") - F.col("data_criacao").cast("long"),
        )
    )
    tempos.createOrReplaceTempView("tempo_decisao_do_lote")

    spark_do_lote.sql(
        f"""
        MERGE INTO delta.`{caminho_tempo_decisao}` AS alvo
        USING tempo_decisao_do_lote AS novo
        ON alvo.id_proposta = novo.id_proposta
        WHEN MATCHED THEN UPDATE SET
            alvo.tipo_produto = novo.tipo_produto,
            alvo.status_decisao = novo.status_decisao,
            alvo.data_criacao = novo.data_criacao,
            alvo.data_decisao = novo.data_decisao,
            alvo.tempo_decisao_segundos = novo.tempo_decisao_segundos
        WHEN NOT MATCHED THEN INSERT (
            id_proposta, tipo_produto, status_decisao, data_criacao, data_decisao, tempo_decisao_segundos
        ) VALUES (
            novo.id_proposta, novo.tipo_produto, novo.status_decisao,
            novo.data_criacao, novo.data_decisao, novo.tempo_decisao_segundos
        )
        """
    )


def escrever_contagem_atual(micro_lote: DataFrame, caminho_historico: str, caminho_contagem_atual: str) -> None:
    if micro_lote.isEmpty():
        return

    spark_do_lote = micro_lote.sparkSession
    abertos = (
        spark_do_lote.read.format("delta")
        .load(caminho_historico)
        .filter(F.col("valido_ate").isNull() & (F.col("status") != "deletada"))
        .groupBy("tipo_produto", "status")
        .agg(F.count("*").alias("quantidade"))
    )
    abertos.write.format("delta").mode("overwrite").save(caminho_contagem_atual)


def processar_lote_metricas(config: ConfigGold, micro_lote: DataFrame, id_lote: int) -> None:
    if micro_lote.isEmpty():
        return

    micro_lote_cache = micro_lote.cache()
    try:
        escrever_tempo_decisao(micro_lote_cache, config.historico_path, config.tempo_decisao_path)
        escrever_contagem_atual(micro_lote_cache, config.historico_path, config.contagem_atual_path)
    finally:
        micro_lote_cache.unpersist()


def construir_queries(spark: SparkSession, config: ConfigGold) -> list:
    garantir_tabelas_gold(spark, config)

    leitura_historico = (
        spark.readStream.format("delta").option("skipChangeCommits", "true").load(config.historico_path)
    )

    agregacao_funil = construir_agregacao_funil(leitura_historico, config.watermark_atraso, config.janela_funil)
    query_funil = (
        agregacao_funil.writeStream.outputMode("update")
        .foreachBatch(lambda lote, id_lote: escrever_funil(lote, config.funil_path))
        .option("checkpointLocation", config.checkpoint_funil_path)
        .trigger(processingTime=config.trigger_intervalo)
        .start()
    )

    leitura_bronze = spark.readStream.format("delta").load(config.bronze_path)
    agregacao_volume_valor = construir_agregacao_volume_valor(
        leitura_bronze, config.watermark_atraso, config.janela_funil
    )
    query_volume_valor = (
        agregacao_volume_valor.writeStream.outputMode("update")
        .foreachBatch(lambda lote, id_lote: escrever_volume_valor(lote, config.volume_valor_path))
        .option("checkpointLocation", config.checkpoint_volume_valor_path)
        .trigger(processingTime=config.trigger_intervalo)
        .start()
    )

    query_metricas = (
        leitura_historico.writeStream.foreachBatch(
            lambda lote, id_lote: processar_lote_metricas(config, lote, id_lote)
        )
        .option("checkpointLocation", config.checkpoint_metricas_path)
        .trigger(processingTime=config.trigger_intervalo)
        .start()
    )

    return [query_funil, query_volume_valor, query_metricas]


def main() -> None:
    config = ConfigGold.from_env()

    spark = SparkSession.builder.appName("gold-propostas").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    spark.conf.set("spark.sql.shuffle.partitions", config.shuffle_partitions)

    aguardar_tabela_disponivel(spark, config.bronze_path)
    aguardar_tabela_disponivel(spark, config.historico_path)

    construir_queries(spark, config)
    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    main()
