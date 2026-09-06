from __future__ import annotations

import os
from dataclasses import dataclass

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, current_timestamp, from_unixtime, get_json_object, to_date
from pyspark.sql.types import StringType


def _str_env(nome: str, padrao: str) -> str:
    valor = os.environ.get(nome)
    return valor if valor else padrao


@dataclass(frozen=True)
class ConfigBronze:
    kafka_bootstrap_servers: str
    kafka_topic: str
    bronze_path: str
    checkpoint_path: str
    trigger_intervalo: str
    shuffle_partitions: str

    @classmethod
    def from_env(cls) -> "ConfigBronze":
        return cls(
            kafka_bootstrap_servers=_str_env("KAFKA_BOOTSTRAP_INTERNO", "kafka:19092"),
            kafka_topic=_str_env("KAFKA_TOPIC_PROPOSTAS", "propostas.cdc"),
            bronze_path=_str_env("BRONZE_PATH", "s3a://bronze/propostas"),
            checkpoint_path=_str_env("BRONZE_CHECKPOINT_PATH", "s3a://bronze/_checkpoints/propostas"),
            trigger_intervalo=_str_env("BRONZE_TRIGGER_INTERVALO", "10 seconds"),
            shuffle_partitions=_str_env("BRONZE_SHUFFLE_PARTITIONS", "6"),
        )


def construir_dataframe_bronze(leitura_kafka: DataFrame) -> DataFrame:
    valor_str = col("value").cast(StringType())

    return (
        leitura_kafka.select(
            col("key").cast(StringType()).alias("kafka_key"),
            col("topic").alias("kafka_topic"),
            col("partition").alias("kafka_partition"),
            col("offset").alias("kafka_offset"),
            col("timestamp").alias("kafka_timestamp"),
            valor_str.alias("envelope_bruto"),
            get_json_object(valor_str, "$.id_proposta").alias("id_proposta"),
            get_json_object(valor_str, "$.operation_type").alias("operation_type"),
            get_json_object(valor_str, "$.cluster_time_epoch").cast("long").alias("cluster_time_epoch"),
            get_json_object(valor_str, "$.resume_token._data").alias("resume_token_data"),
            (get_json_object(valor_str, "$.chave_derivada_de_fallback") == "true").alias(
                "chave_derivada_de_fallback"
            ),
            get_json_object(valor_str, "$.document_key").alias("document_key_json"),
            get_json_object(valor_str, "$.full_document").alias("full_document_json"),
            get_json_object(valor_str, "$.full_document_before_change").alias(
                "full_document_before_change_json"
            ),
            get_json_object(valor_str, "$.update_description").alias("update_description_json"),
        )
        .withColumn("data_evento", to_date(from_unixtime(col("cluster_time_epoch"))))
        .withColumn("data_ingestao", current_timestamp())
    )


def main() -> None:
    config = ConfigBronze.from_env()

    spark = SparkSession.builder.appName("bronze-propostas").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    spark.conf.set("spark.sql.shuffle.partitions", config.shuffle_partitions)

    leitura = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", config.kafka_bootstrap_servers)
        .option("subscribe", config.kafka_topic)
        .option("startingOffsets", "earliest")
        .load()
    )

    bronze = construir_dataframe_bronze(leitura)

    query = (
        bronze.writeStream.format("delta")
        .outputMode("append")
        .option("checkpointLocation", config.checkpoint_path)
        .partitionBy("data_evento")
        .trigger(processingTime=config.trigger_intervalo)
        .start(config.bronze_path)
    )

    query.awaitTermination()


if __name__ == "__main__":
    main()
