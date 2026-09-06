# Roteiro de validação manual — Fase 1

Este roteiro confirma que o ambiente da Fase 1 (MongoDB replica set, Kafka
KRaft, MinIO, Spark, gerador de carga) está funcionando de ponta a ponta em
uma máquina limpa. Rode a partir da raiz do repositório, dentro do WSL2.

## 1. Subir o ambiente do zero

```bash
bash scripts/bootstrap.sh
```

Esperado: todos os serviços sobem saudáveis sem intervenção manual, sem
`sleep` — o script só retorna quando `mongo`, `mongo-init`, `kafka`,
`kafka-init`, `minio`, `minio-init`, `spark-master` e `spark-worker`
reportam `healthy`, e `generator` está em execução. Se algum serviço não
ficar saudável a tempo, o script aponta qual e como ver os logs.

## 2. Confirmar o replica set do MongoDB

```bash
docker compose -f infra/docker-compose.yml --env-file infra/.env \
  exec mongo mongosh --quiet --eval "rs.status().myState"
```

Esperado: `1` (o nó único é PRIMARY).

## 3. Confirmar o tópico Kafka

```bash
docker compose -f infra/docker-compose.yml --env-file infra/.env \
  exec kafka /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 --describe --topic propostas.cdc
```

Esperado: `PartitionCount: 6`, `ReplicationFactor: 1`.

## 4. Confirmar os buckets do MinIO

```bash
docker compose -f infra/docker-compose.yml --env-file infra/.env \
  exec minio-init mc ls local
```

Esperado: `bronze/`, `silver/`, `gold/`.

## 5. Confirmar o gerador de carga

```bash
docker compose -f infra/docker-compose.yml --env-file infra/.env logs -f generator
```

Esperado: logs mostrando propostas sendo criadas (`consignado`, `cartao`,
`fgts`), transições de status, correções retroativas e deletes ocasionais.
Interrompa com Ctrl+C (não derruba o container, só o `logs -f`).

Confirme diretamente no Mongo:

```bash
docker compose -f infra/docker-compose.yml --env-file infra/.env \
  exec mongo mongosh --quiet creditodb --eval "
    print('total de propostas: ' + db.propostas.countDocuments({}));
    print('por tipo_produto:');
    printjson(db.propostas.aggregate([{\$group: {_id: '\$tipo_produto', total: {\$sum: 1}}}]).toArray());
    print('por status:');
    printjson(db.propostas.aggregate([{\$group: {_id: '\$status', total: {\$sum: 1}}}]).toArray());
  "
```

Esperado: documentos dos três tipos de produto, com campos específicos
diferentes por tipo, e distribuição de status coerente com o ciclo de vida
(mais propostas em estados iniciais do que em terminais, já que o ambiente
acabou de subir).

## 6. Smoke test do Spark (Kafka + Delta + S3A)

Este passo confirma que os três conectores embutidos na imagem Spark
funcionam antes de começar a Fase 2/3. Não é o pipeline de streaming real —
é só uma verificação de conectividade da imagem.

```bash
docker compose -f infra/docker-compose.yml --env-file infra/.env \
  exec spark-master bash -c "cat > /tmp/smoke.py" <<'EOF'
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("fase1-smoke-test").getOrCreate()

# 1) Delta Lake + S3A (MinIO): escreve e le uma tabela de teste.
dados = spark.range(5).withColumnRenamed("id", "valor")
dados.write.format("delta").mode("overwrite").save("s3a://bronze/_smoke")
lido = spark.read.format("delta").load("s3a://bronze/_smoke")
assert lido.count() == 5, "delta/s3a: contagem inesperada"
print("OK: delta + s3a (MinIO) funcionando, 5 linhas lidas de volta")

# 2) Kafka: confirma que o conector consegue ler do topico (mesmo que vazio).
leitura_kafka = (
    spark.read.format("kafka")
    .option("kafka.bootstrap.servers", "kafka:19092")
    .option("subscribe", "propostas.cdc")
    .option("startingOffsets", "earliest")
    .load()
)
total_kafka = leitura_kafka.count()
print(f"OK: conector kafka funcionando, {total_kafka} mensagens no topico propostas.cdc")

spark.stop()
EOF

docker compose -f infra/docker-compose.yml --env-file infra/.env \
  exec spark-master /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  /tmp/smoke.py
```

Esperado: as duas linhas `OK: ...` impressas, sem exceções. Se a etapa 1
falhar, o problema está na config S3A/Delta (`infra/spark/conf/spark-defaults.conf`)
ou nas credenciais do MinIO; se a etapa 2 falhar, o problema está no
conector Kafka embutido na imagem ou nos listeners do broker.

Acompanhe visualmente pela Spark UI em <http://localhost:8080> (master) e
<http://localhost:8081> (worker) enquanto o job roda.

## 7. Teardown e nova subida (idempotência)

```bash
bash scripts/teardown.sh
bash scripts/bootstrap.sh
```

Esperado: o ambiente sobe limpo novamente, sem erros, com um novo
`KAFKA_CLUSTER_ID` gerado automaticamente (o antigo foi descartado junto
com o volume `kafka-data`).
