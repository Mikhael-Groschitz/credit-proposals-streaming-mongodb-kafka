# Streaming de propostas de crédito — MongoDB, Kafka e Spark

Pipeline de dados que captura mudanças de estado de propostas de crédito
(consignado, cartão e FGTS) gravadas em MongoDB, propaga essas mudanças via
change streams para o Kafka, e as materializa em camadas bronze/silver/gold
sobre Delta Lake, usando Spark Structured Streaming de ponta a ponta.

Este é o quarto projeto do meu portfólio de Engenharia de Dados. Construí
tudo sem nenhum serviço de nuvem pago — todo componente roda em Docker
Compose, na mesma rede.

## Status do projeto

Estou construindo isso em fases, validando cada uma de ponta a ponta antes
de passar para a próxima. Por enquanto:

- [x] **Fase 1 — Ambiente e fonte operacional**: ambiente Docker completo e
      gerador de carga operacional
- [x] **Fase 2 — Conector de change streams**: resume token durável,
      `fullDocument`/`fullDocumentBeforeChange`, publicação no Kafka com
      confirmação antes do checkpoint, roteiro de resiliência
- [x] **Fase 3 — Bronze em streaming**: Spark Structured Streaming lendo o
      Kafka e gravando em Delta no MinIO, particionado por data do evento,
      com checkpoint próprio
- [x] **Fase 4 — Silver e estado atual**: histórico de mudanças (SCD2),
      estado atual via merge, watermark com deduplicação, tratamento de
      delete e de heterogeneidade de schema — cada um provado com teste
- [ ] Fase 5 — Gold, qualidade e observabilidade

A comparação change streams × CDC do SQL Server e a seção "Resiliência"
consolidada (olhando o pipeline inteiro, não só uma peça) ficaram para a
Fase 5 — fazem mais sentido como fechamento depois que o pipeline completo
existir, em vez de forçadas numa fase que ainda não tinha gold nem
qualidade prontos.

## Arquitetura de ponta a ponta (alvo do projeto completo)

```mermaid
flowchart LR
    subgraph f1["Fase 1 — pronto"]
        GEN["Gerador de carga\noperacional (Python)"]
    end
    GEN -->|insert / update / delete / replace| MONGO[("MongoDB\nreplica set rs0")]

    subgraph f2["Fase 2 — pronto"]
        CS["Conector de\nchange streams (Python)"]
    end
    MONGO -->|change stream| CS
    CS -->|chave = id_proposta| KAFKA[("Kafka\ntópico propostas.cdc")]

    subgraph f3["Fase 3 — pronto"]
        BRONZE["Spark Structured Streaming\nBronze"]
    end
    KAFKA --> BRONZE
    BRONZE -->|Delta| LAKE_B[("MinIO: bronze")]

    subgraph f4["Fase 4 — pronto"]
        SILVER["Spark: histórico de mudanças\n+ estado atual (merge)"]
    end
    LAKE_B --> SILVER
    SILVER -->|Delta merge| LAKE_S[("MinIO: silver")]

    subgraph f5["Fase 5 — planejado"]
        GOLD["Spark: agregações em janela\n+ checks de qualidade"]
    end
    LAKE_S --> GOLD
    GOLD -->|Delta| LAKE_G[("MinIO: gold")]
    LAKE_G --> DUCK["DuckDB\nconsultas de exemplo"]
```

## Modelo de dados e ciclo de vida (definido para o projeto inteiro)

Uma proposta tem campos comuns a qualquer tipo de produto, mais campos
específicos por tipo:

| Comuns | Consignado | Cartão | FGTS |
|---|---|---|---|
| `id_proposta`, `tipo_produto`, `cpf_cliente`, `nome_cliente`, `valor_solicitado`, `status`, `data_criacao`, `data_atualizacao` | `matricula`, `orgao_convenio`, `prazo_meses`, `taxa_juros`, `margem_consignavel` | `limite_solicitado`, `bandeira`, `score_credito` | `saldo_fgts_disponivel`, `percentual_antecipado`, `banco_parceiro` |

`id_proposta` é a chave de negócio da proposta e, desde a Fase 2, também a
chave de cada mensagem no Kafka — garante que todas as mudanças de uma
mesma proposta caem na mesma partição e mantêm ordem.

Ciclo de vida de status:

```mermaid
stateDiagram-v2
    [*] --> em_analise
    em_analise --> pendente_documentacao
    pendente_documentacao --> em_analise
    em_analise --> aprovada
    pendente_documentacao --> aprovada
    em_analise --> recusada
    pendente_documentacao --> recusada
    em_analise --> cancelada
    pendente_documentacao --> cancelada
    aprovada --> paga
    aprovada --> cancelada
    aprovada --> [*]
    paga --> [*]
    recusada --> [*]
    cancelada --> [*]
```

`recusada`, `paga` e `cancelada` são estados terminais. Além das transições
de status, o gerador de carga também produz:

- **Deletes**: exclusão de uma proposta criada por engano (erro
  operacional simulado) — remoção real do documento no Mongo.
- **Correções retroativas**: `replace` de um campo de negócio
  (`valor_solicitado`, `nome_cliente` ou `cpf_cliente`) sem alterar
  `status`, mas atualizando `data_atualizacao` (é tratada como qualquer
  escrita de auditoria — só não mexe no status da proposta).

## Fase 1 — o que existe hoje

### Ambiente Docker

Coloquei todo serviço no Compose, na mesma rede `credpipe-net`:

| Serviço | Papel |
|---|---|
| `mongo` + `mongo-init` | MongoDB `8.0.17` como replica set de nó único (`rs0`), iniciado automaticamente |
| `kafka` + `kafka-init` | Kafka `4.3.1` em modo KRaft (broker+controller combinado, sem ZooKeeper), tópico `propostas.cdc` (6 partições) |
| `minio` + `minio-init` | MinIO com os buckets `bronze`, `silver`, `gold` já criados |
| `spark-master` + `spark-worker` | Spark `3.5.8` standalone, imagem própria com os conectores Kafka + Delta Lake + S3A já embutidos |
| `generator` | Gerador de carga operacional (Python) |
| `connector` | Conector de change streams (Python, Fase 2) |

Os contêineres `*-init` existem porque o healthcheck de um serviço confirma
só que o processo está de pé — não que o replica set foi criado, ou que o
tópico/buckets existem. Cada `*-init` roda seu trabalho de forma idempotente
e fica vivo depois, sinalizando prontidão via um arquivo-sentinela que o
Compose usa como `condition: service_healthy`. Nenhum `sleep` fixo em
nenhum ponto da cadeia de inicialização — ver a seção "Decisões e
trade-offs", abaixo, para as versões exatas de cada imagem e por quê.

Ordem de inicialização:

```mermaid
flowchart TD
    mongo -- healthy --> mongo_init[mongo-init]
    mongo_init -- healthy --> generator
    mongo_init -- healthy --> connector
    kafka -- healthy --> kafka_init[kafka-init]
    kafka_init -- healthy --> spark_master[spark-master]
    kafka_init -- healthy --> connector
    minio -- healthy --> minio_init[minio-init]
    minio_init -- healthy --> spark_master
    spark_master -- healthy --> spark_worker[spark-worker]
```

### Gerador de carga operacional

Escrevi um gerador que simula o sistema operacional gravando propostas no
Mongo: cria propostas dos três tipos, evolui o status conforme a máquina
de estados acima, e produz deletes e correções retroativas, tudo em loop
com taxa configurável. Código em [`generator/`](generator/), configurado
inteiramente por
variáveis de ambiente (ver [`infra/.env.example`](infra/.env.example)):

| Variável | Papel |
|---|---|
| `EVENTS_PER_SECOND` | taxa alvo de operações por segundo |
| `RATIO_CONSIGNADO` / `RATIO_CARTAO` / `RATIO_FGTS` | peso relativo de cada tipo em propostas novas |
| `RATIO_NEW_VS_TRANSITION` | fração dos eventos que cria proposta nova vs. avança uma existente |
| `RATIO_DELETE` | fração das propostas recém-criadas marcadas para exclusão futura |
| `RATIO_CORRECAO` | fração dos eventos de transição que viram correção retroativa |
| `RANDOM_SEED` | opcional, fixa a semente para reproduzir a mesma sequência |

## Fase 2 — o que existe hoje

### Conector de change streams

Escrevi um serviço Python (`connector/`) que abre um change stream na
coleção `propostas`, monta um envelope JSON por evento e publica no tópico
`propostas.cdc` com `id_proposta` como chave da mensagem. Roda como serviço
próprio no Compose, depende de `mongo-init` e `kafka-init` saudáveis.

O envelope publicado inclui `id_proposta`, `operation_type`, `cluster_time`,
`cluster_time_epoch` (o mesmo instante, já como inteiro Unix — evita a
Fase 3 ter que decodificar o formato `{"$timestamp": {...}}` do extended
JSON só para particionar por data), `resume_token`, `document_key`,
`full_document`, `full_document_before_change` e `update_description` —
serializados com `bson.json_util` (extended JSON relaxado), para que a
Fase 3 consiga ler o JSON diretamente sem reimplementar a decodificação de
tipos BSON (`ObjectId`, `datetime`, `Timestamp`).

| Variável | Papel |
|---|---|
| `KAFKA_DELIVERY_TIMEOUT_MS` / `KAFKA_REQUEST_TIMEOUT_MS` | timeouts do producer usados para detectar broker indisponível em segundos |
| `CHECKPOINT_COLLECTION` / `CHECKPOINT_ID` | onde o resume token é persistido (dentro do próprio MongoDB) |
| `BATCH_MAX_SIZE` / `BATCH_MAX_INTERVALO_MS` | tamanho/intervalo máximo de um lote antes de publicar e fazer checkpoint |
| `RETRY_BACKOFF_INICIAL_MS` / `RETRY_BACKOFF_MAXIMO_MS` | backoff exponencial ao perder conexão com Mongo ou Kafka |

O roteiro completo de resiliência (reinício do conector, queda do Kafka,
queda do Mongo, e resume token inválido/corrompido) está na seção
"Roteiro de resiliência (Fase 2)", mais abaixo.

## Fase 3 — o que existe hoje

### Bronze em streaming

Escrevi um job Spark Structured Streaming (`spark-jobs/bronze/bronze_job.py`)
que lê o tópico `propostas.cdc` continuamente e grava em Delta no MinIO
(`s3a://bronze/propostas`), particionado por `data_evento` (a data derivada
de `cluster_time_epoch`, não a data de ingestão). Roda como serviço próprio
no Compose (`bronze`), submetido via `spark-submit` contra o cluster
standalone (`spark://spark-master:7077`) — nada de rodar do host.

Nenhuma transformação de negócio: o job desserializa só os campos estáveis
do envelope (`id_proposta`, `operation_type`, `cluster_time_epoch`,
`resume_token_data`, metadados do Kafka) como colunas tipadas, e mantém
`full_document`, `full_document_before_change`, `update_description` e
`document_key` como colunas de texto JSON — sem tentar achatar os campos
que variam por `tipo_produto`. Isso é trabalho da Fase 4. Também mantém
`envelope_bruto` (a mensagem Kafka original, sem nenhum parsing) como
coluna própria, garantindo fidelidade total mesmo se algum parsing
específico tiver um bug.

| Variável | Papel |
|---|---|
| `BRONZE_PATH` / `BRONZE_CHECKPOINT_PATH` | caminho da tabela Delta e do checkpoint da Structured Streaming no MinIO |
| `BRONZE_TRIGGER_INTERVALO` | intervalo do micro-batch nativo do motor de streaming do Spark |
| `BRONZE_SHUFFLE_PARTITIONS` | número de partições de shuffle (baixo de propósito, dado o volume pequeno do gerador) |

### Rodando o smoke test do bronze

```bash
bash scripts/bootstrap.sh
# aguardar o gerador produzir alguns eventos e o conector publicá-los
docker compose -f infra/docker-compose.yml --env-file infra/.env logs -f bronze
```

Para inspecionar a tabela Delta diretamente (não pelo cluster — ver "por
que `local[2]`" nas decisões abaixo):

```bash
cat > /tmp/verificar_bronze.py <<'EOF'
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("verificar-bronze").master("local[2]").getOrCreate()
df = spark.read.format("delta").load("s3a://bronze/propostas")
print(f"total de linhas: {df.count()}")
df.groupBy("operation_type").count().show()
EOF
docker compose -f infra/docker-compose.yml --env-file infra/.env cp /tmp/verificar_bronze.py spark-master:/tmp/verificar_bronze.py
docker compose -f infra/docker-compose.yml --env-file infra/.env exec -T spark-master \
  /opt/spark/bin/spark-submit /tmp/verificar_bronze.py
```

Validei rodando de verdade: a tabela particiona corretamente por
`data_evento`, a distribuição por `operation_type` bate com o que o
gerador produz, `full_document_json` preserva os campos específicos de
cada tipo de produto, e um `delete` chega com `full_document_json` nulo e
`full_document_before_change_json` populado — confirmando que a pré-imagem
configurada na Fase 2 sobrevive o caminho inteiro até o bronze. Reiniciar
o serviço `bronze` retoma do checkpoint (offset incrementando sem reset),
sem reprocessar nem perder micro-lotes.

## Fase 4 — o que existe hoje

### Silver: histórico de mudanças e estado atual

Escrevi um job Spark Structured Streaming (`spark-jobs/silver/silver_job.py`)
que lê a tabela bronze **como stream** (Delta é fonte e destino ao mesmo
tempo — o encadeamento clássico de arquitetura medalhão) e materializa
duas visões Delta, escritas no mesmo `foreachBatch` a partir do mesmo lote:

- **`s3a://silver/historico`**: uma linha por intervalo de validade de
  status (`valido_de`/`valido_ate`, SCD tipo 2). Só transições de status
  reais geram linha nova — uma correção retroativa (que não muda status)
  não aparece aqui.
- **`s3a://silver/estado_atual`**: uma linha por `id_proposta` com a
  versão mais recente conhecida, atualizada via `MERGE INTO` — insere,
  atualiza ou remove (em caso de delete) conforme o evento.

Antes de chegar nessas duas escritas, o stream lido do bronze passa por
`.withWatermark("hora_evento", SILVER_WATERMARK_ATRASO)` seguido de
`.dropDuplicatesWithinWatermark(["resume_token_data"])` — a deduplicação
que a Fase 2 deixou pendente (o conector garante at-least-once, não
exatamente-uma-vez) acontece aqui, pela chave natural do próprio evento do
MongoDB.

`full_document`/`full_document_before_change` (que o bronze guardou como
texto JSON intacto) são finalmente processados aqui: os campos comuns a
qualquer tipo de produto viram colunas tipadas em `estado_atual`
(`cpf_cliente`, `nome_cliente`, `valor_solicitado`, `status`,
`data_criacao`, `data_atualizacao`), e tudo que sobra — os campos
específicos de consignado, cartão ou FGTS — fica preservado, sem perda de
informação, numa coluna `campos_especificos_json`.

| Variável | Papel |
|---|---|
| `SILVER_HISTORICO_PATH` / `SILVER_ESTADO_ATUAL_PATH` | caminho das duas tabelas Delta de saída |
| `SILVER_CHECKPOINT_PATH` | checkpoint único da streaming query (um job, dois sinks) |
| `SILVER_WATERMARK_ATRASO` | janela de tolerância para deduplicar por `resume_token_data` |
| `SILVER_TRIGGER_INTERVALO` / `SILVER_SHUFFLE_PARTITIONS` | mesmo papel que na Fase 3 |

### Testando de verdade: pytest local + streaming real via Docker

Separei em `spark-jobs/silver/transformacoes.py` as duas únicas peças de
lógica que são Python puro (extrair `campos_especificos_json`; decidir se
um evento é transição de status) — assim consigo testá-las num venv
comum, sem precisar de Spark:

```bash
cd spark-jobs
python -m venv .venv && source .venv/bin/activate
pip install -r requirements-dev.txt
pytest silver/tests/test_transformacoes.py -q
```

Já a lógica de streaming (watermark, deduplicação, os dois `MERGE INTO`)
só faz sentido testada com Spark de verdade — isso exige JVM, o que este
projeto não tem fora de Docker. `spark-jobs/silver/tests/test_silver_job.py`
sobe uma `SparkSession` local dentro da própria imagem `credpipe/spark`,
escreve lotes num Delta temporário simulando o bronze, e chama
`query.processAllAvailable()` (o utilitário oficial do Spark para testar
streaming de forma síncrona e determinística) entre cada lote:

```bash
docker run --rm --user root \
  -v "<caminho-absoluto-do-repo>/spark-jobs:/work" \
  -e HOME=/tmp \
  -e PYTHONPATH="/opt/spark/python:/opt/spark/python/lib/py4j-0.10.9.7-src.zip:/opt/spark/python/lib/pyspark.zip" \
  -w /work/silver \
  credpipe/spark:3.5.8 sh -c "pip3 install -q pytest && python3 -m pytest tests/ -q"
```

Os testes provam, com dados de verdade passando pelo pipeline real (não
mocks): que uma transição de status fecha o intervalo anterior no
histórico e abre um novo; que uma correção retroativa atualiza o
`estado_atual` sem criar linha no histórico; que um delete remove a
proposta do `estado_atual` e fecha o histórico com `status = 'deletada'`;
que uma duplicata exata do mesmo `resume_token_data` dentro da janela do
watermark é descartada; e que um reprocessamento atrasado (fora da janela)
não regride o `estado_atual` graças ao guard de timestamp no merge.

Validei também no ambiente real: o job processou o backlog do bronze
corretamente (histórico com 6 mil+ linhas, transições de status em
sequência coerente por proposta, `campos_especificos_json` correto por
tipo de produto), zero propostas deletadas sobrando no `estado_atual`, e
reiniciar o serviço `silver` retomou do checkpoint sem resetar.

## Como executar (Linux / WSL2 + Docker Desktop)

Pré-requisitos: Docker Desktop com integração WSL2 habilitada, rodando
dentro de uma distro WSL2 (os scripts assumem bash/paths Unix).

```bash
git clone <url-do-repositorio>
cd projeto-mongodb-streaming

# sobe tudo do zero (cria infra/.env, gera o CLUSTER_ID do Kafka, builda as
# imagens custom na primeira vez, sobe os serviços, espera todos saudáveis)
bash scripts/bootstrap.sh
```

Ao final, o script imprime as URLs úteis:

- MongoDB: `mongodb://localhost:27017/?replicaSet=rs0`
- MinIO console: <http://localhost:9001>
- Spark UI: <http://localhost:8080>
- Kafka: `localhost:9092`, tópico `propostas.cdc`

Para derrubar tudo (e limpar os volumes, deixando pronto para uma próxima
subida 100% do zero):

```bash
bash scripts/teardown.sh
```

### Roteiro de validação manual (Fase 1)

Uso este roteiro para confirmar que o ambiente sobe de ponta a ponta numa
máquina limpa. Todos os comandos abaixo usam:

```bash
docker compose -f infra/docker-compose.yml --env-file infra/.env <comando>
```

**1. Subir o ambiente do zero** (`bash scripts/bootstrap.sh`) — esperado:
todos os serviços saudáveis sem intervenção manual e sem `sleep`; o script
só retorna quando `mongo`, `mongo-init`, `kafka`, `kafka-init`, `minio`,
`minio-init`, `spark-master` e `spark-worker` reportam `healthy`, e
`generator` está em execução.

**2. Confirmar o replica set do MongoDB**:

```bash
... exec mongo mongosh --quiet --eval "rs.status().myState"
```

Esperado: `1` (o nó único é PRIMARY).

**3. Confirmar o tópico Kafka**:

```bash
... exec kafka /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 --describe --topic propostas.cdc
```

Esperado: `PartitionCount: 6`, `ReplicationFactor: 1`.

**4. Confirmar os buckets do MinIO**: `... exec minio-init mc ls local` —
esperado `bronze/`, `silver/`, `gold/`.

**5. Confirmar o gerador de carga**: `... logs -f generator` — esperado
logs mostrando propostas sendo criadas (`consignado`, `cartao`, `fgts`),
transições de status, correções retroativas e deletes ocasionais. Direto
no Mongo:

```bash
... exec mongo mongosh --quiet creditodb --eval "
  print('total de propostas: ' + db.propostas.countDocuments({}));
  printjson(db.propostas.aggregate([{\$group: {_id: '\$tipo_produto', total: {\$sum: 1}}}]).toArray());
  printjson(db.propostas.aggregate([{\$group: {_id: '\$status', total: {\$sum: 1}}}]).toArray());
"
```

Esperado: documentos dos três tipos de produto, campos específicos
diferentes por tipo, e distribuição de status coerente com o ciclo de vida
(mais propostas em estados iniciais do que terminais logo após subir).

**6. Smoke test do Spark (Kafka + Delta + S3A)** — confirma que os três
conectores embutidos na imagem funcionam, antes de existir pipeline de
streaming real:

```bash
... exec spark-master bash -c "cat > /tmp/smoke.py" <<'EOF'
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("fase1-smoke-test").getOrCreate()

dados = spark.range(5).withColumnRenamed("id", "valor")
dados.write.format("delta").mode("overwrite").save("s3a://bronze/_smoke")
lido = spark.read.format("delta").load("s3a://bronze/_smoke")
assert lido.count() == 5, "delta/s3a: contagem inesperada"
print("OK: delta + s3a (MinIO) funcionando, 5 linhas lidas de volta")

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

... exec spark-master /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 /tmp/smoke.py
```

Esperado: as duas linhas `OK: ...` impressas, sem exceções. Se a etapa 1
falhar, o problema está na config S3A/Delta
(`infra/spark/conf/spark-defaults.conf`) ou nas credenciais do MinIO; se a
etapa 2 falhar, o problema está no conector Kafka embutido na imagem ou
nos listeners do broker. Acompanhe pela Spark UI em
<http://localhost:8080> (master) e <http://localhost:8081> (worker)
enquanto o job roda.

**7. Teardown e nova subida (idempotência)**: `bash scripts/teardown.sh`
seguido de `bash scripts/bootstrap.sh` — esperado: ambiente sobe limpo de
novo, sem erros, com um novo `KAFKA_CLUSTER_ID` gerado automaticamente (o
antigo foi descartado junto com o volume `kafka-data`).

### Rodando os testes automatizados do gerador

```bash
cd generator
python -m venv .venv && source .venv/bin/activate
pip install -r requirements-dev.txt
pytest -q
```

Os testes cobrem a máquina de estados (nenhuma transição fora do ciclo
definido, estados terminais não têm saída), a validação de configuração
(variáveis de ambiente fora do intervalo esperado derrubam o serviço com
erro claro em vez de rodar com comportamento errado) e o contrato de que
uma correção retroativa nunca envia o campo `status` ao Mongo.

### Rodando os testes automatizados do conector

```bash
cd connector
python -m venv .venv && source .venv/bin/activate
pip install -r requirements-dev.txt
pytest -q
```

Os testes cobrem a extração de `id_proposta` (do `fullDocument`, do
`fullDocumentBeforeChange` em deletes, e o fallback para `documentKey._id`
quando nenhum dos dois está disponível), o backoff exponencial, o
repositório de checkpoint, e a detecção de falha de entrega no Kafka —
tudo com dublês (`ProdutorFalso`, `ColecaoFalsa`), sem precisar de Mongo ou
Kafka reais rodando. A resiliência de verdade (reconexão, backoff sob
falha real, resume token inválido) só se prova rodando o ambiente de
verdade e derrubando cada peça — é o que o roteiro abaixo documenta.

### Roteiro de resiliência (Fase 2)

Rodei os três cenários exigidos, mais um extra, de verdade contra o
ambiente com Fase 1 + Fase 2 no ar. Comandos no formato:

```bash
docker compose -f infra/docker-compose.yml --env-file infra/.env <comando>
```

**Cenário 1 — reinício do próprio conector**:

```bash
... exec mongo mongosh --quiet creditodb --eval \
  "db.checkpoints.findOne().resume_token._data"
... restart connector
... logs --tail=20 connector
```

Observado: o conector recebe SIGTERM, loga "sinal 15 recebido, encerrando
após o lote em andamento...", publica e confirma o lote que estava em
memória antes de sair, e loga "conector encerrado.". Na inicialização
seguinte, loga "retomando a partir do resume token persistido" e volta a
publicar sem lacunas na sequência de `resume_token`. Na pior hipótese (o
processo morrer entre a confirmação no Kafka e a gravação do checkpoint),
o pior caso é reprocessar o último lote — nunca perder eventos.

**Cenário 2 — queda do Kafka**:

```bash
... stop kafka
... logs --tail=20 connector   # aguardar ~15-20s
... start kafka
... logs --tail=20 connector
```

Observado ao derrubar: em poucos segundos (não minutos) o producer reporta
falha de entrega (`KafkaError{code=_MSG_TIMED_OUT}`), o conector loga
"falha ao publicar lote no Kafka (...), tentando novamente em Xs..." com
backoff exponencial (1s, 2s, 4s, 8s...), e continua tentando o **mesmo**
lote em memória sem avançar o cursor do MongoDB — confirmei observando que
`db.propostas` continua recebendo escritas do gerador normalmente durante
toda a queda, sem qualquer relação com o estado do conector. Observado ao
religar: o conector reconecta sozinho e publica, em sequência rápida, os
lotes acumulados durante a queda (no tamanho máximo configurado, drenando
o backlog), voltando ao ritmo normal em seguida. Nenhum evento gerado
durante a queda foi perdido.

**Cenário 3 — queda do MongoDB**:

```bash
... stop mongo
... logs --tail=20 connector   # aguardar ~30-40s (selecao de servidor do driver)
... start mongo
... logs --tail=20 connector
```

Observado ao derrubar: o conector loga "MongoDB indisponível
(...AutoReconnect...), tentando novamente em Xs..." com o mesmo backoff
exponencial, sem derrubar o processo — a detecção leva ~30s porque é
regida pelo `serverSelectionTimeoutMS` padrão do driver pymongo. Observado
ao religar: assim que o replica set volta a responder, o conector reabre o
change stream a partir do último resume token persistido e volta a
publicar normalmente — como o volume `mongo-data` preserva a configuração
do replica set, não é necessário rodar `rs.initiate()` de novo.

**Cenário extra — resume token inválido/corrompido**: não é um dos três
pedidos, mas é o comportamento mais importante de provar — o requisito
explícito é "se o token ficar mais antigo que o oplog disponível, falhar
com mensagem clara pedindo recarga, em vez de pular eventos
silenciosamente":

```bash
... exec mongo mongosh --quiet creditodb --eval "
db.checkpoints.updateOne(
  {_id: 'conector-propostas'},
  {\$set: {resume_token: {_data: 'token-invalido'}}}
)
"
... up -d --force-recreate connector
... logs --tail=15 connector
... ps -a connector
```

Observado: o conector levanta `ResumeTokenInvalidoError` com uma mensagem
explícita nomeando a coleção e o `_id` do checkpoint a remover, o processo
termina com código de saída 1, e o container **fica parado** (não entra em
crash-loop) — o serviço `connector` usa `restart: "no"` exatamente para
que essa falha fique visível via `docker compose ps`, em vez de se perder
numa sequência infinita de reinícios. Nenhum evento é pulado
silenciosamente: o conector recusa avançar sem um token utilizável. Para
recuperar (decisão consciente, não automática):

```bash
... exec mongo mongosh --quiet creditodb --eval \
  "db.checkpoints.deleteOne({_id: 'conector-propostas'})"
... up -d connector
```

Sem checkpoint, o conector reinicia a partir de "agora" — eventos gerados
entre a invalidação do token e a limpeza do checkpoint são perdidos. Essa
é uma decisão do operador, feita olhando `docker compose ps`/logs, nunca
uma decisão automática do código.

## Decisões e trade-offs (Fase 1)

**Coloquei tudo em Docker Compose, nada solto no host.** O replica set do
MongoDB anuncia seus membros pelo hostname interno da rede do Compose
(`mongo`, ver `infra/mongo/init-replica-set.sh`). Um client rodando fora
dessa rede consegue abrir a conexão inicial (a porta 27017 está exposta),
mas ao descobrir a topologia do replica set recebe esse hostname interno e
falha ao tentar alcançá-lo — um erro difícil de diagnosticar porque a
conexão parece funcionar por um instante. Preferi manter todo client dentro
da rede a contorná-lo com configuração especial de DNS no host.

**Fiz o replica set de nó único já na Fase 1, mesmo sem consumidor ainda.**
Change streams do MongoDB são implementados sobre o oplog, que só existe em
replica sets. Um MongoDB standalone não tem de onde emitir os eventos que a
Fase 2 vai consumir — por isso o `mongo-init` já roda `rs.initiate()`
automaticamente, antes mesmo de o conector de change streams existir.

**Escolhi imagens oficiais, não Bitnami.** O catálogo gratuito do Bitnami
foi descontinuado em agosto/2025 (imagens antigas migraram para
`bitnamilegacy`, sem atualização) — muito material de referência sobre
Kafka/Spark/MinIO em Docker ainda assume imagens Bitnami ou versões antigas
do Spark. Fixei um conjunto de versões oficiais (docker-library, projeto
Apache ou MinIO Inc.) e mutuamente compatíveis antes de escrever qualquer
Dockerfile ou compose:

| Componente | Imagem | Versão |
|---|---|---|
| MongoDB | `mongo` | `8.0.17` |
| Kafka | `apache/kafka` | `4.3.1` |
| MinIO server | `minio/minio` | `RELEASE.2025-09-07T16-13-09Z-cpuv1` |
| MinIO client | `minio/mc` | `RELEASE.2025-08-13T08-35-41Z-cpuv1` |
| Spark | `spark` (docker-library) | `3.5.8-scala2.12-java11-python3-ubuntu` |
| Delta Lake | `delta-spark_2.12` | `3.3.2` |
| Conector Kafka p/ Spark | `spark-sql-kafka-0-10_2.12` | `3.5.8` |
| Hadoop AWS (S3A) | `hadoop-aws` | `3.3.4` |
| AWS SDK bundle | `aws-java-sdk-bundle` | `1.12.262` |

Regras de compatibilidade por trás desses pares fixos:

- `spark-sql-kafka-0-10_2.12` **tem que** ser a mesma versão exata do
  Spark — não é versionado independentemente, é publicado junto de cada
  release.
- `delta-spark:3.3.2` é o build oficial testado contra toda a linha
  Spark 3.5.x (Scala 2.12). Subir para Spark 4.0 exigiria Delta 4.x
  (Scala 2.13 only, JDK 17 mínimo) — salto de compatibilidade maior do
  que o necessário para este projeto.
- `hadoop-aws:3.3.4` foi compilado e testado contra exatamente
  `aws-java-sdk-bundle:1.12.262`. Outra combinação causa
  `NoSuchMethodError`/`ClassNotFoundException` só no primeiro acesso a
  `s3a://`, não no build da imagem — por isso trato o par como atômico,
  nunca atualizo um sem o outro.

Tags de patch (`8.0.17`, `3.5.8`, releases do MinIO) evoluem com o tempo —
se ao reconstruir a imagem numa data bem posterior a setembro/2026 uma tag
não existir mais, confirmo com `docker manifest inspect <imagem>:<tag>` e
escolho o patch mais próximo dentro da mesma linha menor (ex.: `3.5.x`),
atualizando `SPARK_VERSION`/`SPARK_IMAGE_TAG` juntos no Dockerfile. E
`minio/minio` removeu o `curl` da imagem em releases recentes, então todo
healthcheck contra MinIO usa `mc ready local`, nunca
`curl .../minio/health/live`.

**Assei os JARs do Spark na imagem, nunca uso `--packages` em runtime.**
Baixar as dependências do Maven Central a cada `spark-submit` exige
internet disponível toda vez e é fonte comum de falha silenciosa (timeout,
proxy, Maven Central fora do ar). O Dockerfile do Spark resolve a árvore
transitiva do conector Kafka e do Delta Lake uma única vez, em tempo de
`docker build`, e congela os `.jar` resultantes na imagem.

**Reservei `id_proposta` como chave da futura mensagem Kafka.** Ainda não
havia producer Kafka na Fase 1, mas já modelei `id_proposta` como
identificador estável justamente para isso: garantir que todas as
mudanças de uma mesma proposta caiam na mesma partição e preservem ordem.
O conector da Fase 2 usa exatamente esse campo como chave, sem precisar
adaptar nada no modelo de dados.

**Já gero deletes e correções na Fase 1, não só a partir da Fase 2.** Mesmo
sem consumidor de change streams ainda, o gerador já produz esses dois
padrões de evento porque prefiro validá-los cedo (via os testes em
`generator/tests/`) a descobrir só na Fase 4 que a semântica de "correção
não muda status, mas atualiza `data_atualizacao`" ficou ambígua.

## Decisões e trade-offs (Fase 2)

**Habilitei `fullDocumentBeforeChange` desde a Fase 1, mesmo sem o conector
existir ainda.** Por padrão, um evento de `update` do change stream traz só
o delta (`updateDescription.updatedFields`), não o documento inteiro — por
isso configurei `full_document="updateLookup"`, que busca o documento
completo no momento da leitura. Só que isso não resolve `delete`: um
delete não tem "documento atual" nenhum para buscar. Para conseguir extrair
`id_proposta` (e qualquer outro campo) de um delete, habilitei também
`full_document_before_change="whenAvailable"`, que traz o documento como
ele era *antes* da mudança — isso exige `changeStreamPreAndPostImages`
habilitado na coleção (`infra/mongo/init-replica-set.sh` já faz isso via
`createCollection`/`collMod`). **Custo real, não hipotético**: cada
update/delete numa coleção monitorada grava uma cópia extra do documento
numa coleção interna (`config.system.preimages`) — dobra o I/O de escrita
para gravar essas pré-imagens. Aceitei esse custo porque, sem ele, todo
delete perderia `id_proposta` (o `documentKey` do change stream só traz o
`_id` interno do Mongo, não campos de negócio) e cairia no fallback
descrito abaixo.

**`id_proposta` tem um fallback documentado, não é garantido 100% das
vezes.** Se `fullDocument`/`fullDocumentBeforeChange` vierem nulos por
qualquer motivo (ex.: pré-imagem expirou, ou uma corrida rara em que o
documento já foi modificado de novo no momento do `updateLookup`), o
conector usa `documentKey._id` como chave e marca
`chave_derivada_de_fallback: true` no envelope — prefiro isso a derrubar o
pipeline ou inventar um `id_proposta` falso.

**Resume token persistido dentro do próprio MongoDB, não em arquivo.**
Guardo o token numa coleção (`checkpoints`) no mesmo banco `creditodb`, não
num volume Docker separado. Como o conector só observa a coleção
`propostas` especificamente (não o banco inteiro), escritas na coleção de
checkpoint não entram no próprio change stream que ele está lendo — sem
esse detalhe, seria um problema circular. A vantagem: não preciso gerenciar
mais um volume, e o checkpoint herda a mesma durabilidade do replica set
que já preciso ter de qualquer forma.

**Resume token só avança depois que o Kafka confirma o lote inteiro.** O
conector lê eventos do Mongo, acumula um lote (por tamanho ou por tempo,
o que vier primeiro — `BATCH_MAX_SIZE`/`BATCH_MAX_INTERVALO_MS`), publica
todos, espera a confirmação de entrega (`flush()` do `confluent-kafka`), e
só então grava o checkpoint com o resume token do último evento do lote.
Se o processo morrer entre a confirmação do Kafka e a gravação do
checkpoint, o pior caso na reinicialização é reprocessar esse lote — nunca
perder um evento. Isso é at-least-once, não exatamente-uma-vez: escolhi
`enable.idempotence=True` no producer (evita duplicar por causa de retry
interno do próprio producer), mas duplicação de ponta a ponta em caso de
crash é uma possibilidade aceita — a Fase 4 vai lidar com isso via
deduplicação no merge do Delta, não aqui.

**Escolhi `confluent-kafka` em vez de `kafka-python`.** O `kafka-python`
original está sem atividade de manutenção há anos, e o fork que tentou
mantê-lo vivo foi arquivado em 2025. `confluent-kafka` publica wheels
pré-compilados (`manylinux`) que instalam direto via pip em
`python:3.11-slim`, sem precisar de toolchain de compilação na imagem, e
tem suporte de primeira classe para os requisitos deste conector
(`acks=all`, confirmação de entrega antes de checkpoint, timeouts finos
para detectar broker fora do ar em segundos).

**O serviço `connector` não reinicia sozinho (`restart: "no"`).** Um resume
token inválido/expirado é, por definição, um erro que não se resolve
tentando de novo automaticamente — exige decisão humana (reload). Deixei o
container parar e ficar visivelmente parado (`docker compose ps` mostra
`Exited`) em vez de entrar num crash-loop silencioso disfarçando o
problema.

## Decisões e trade-offs (Fase 3)

**Bronze não achata `full_document` em colunas — mantém como texto JSON.**
Consignado, cartão e FGTS têm campos diferentes, e um evento de `delete`
não tem `full_document` nenhum (só `full_document_before_change`). Se eu
tentasse aplicar um schema Spark rígido nessas colunas agora, um campo novo
em qualquer tipo de produto quebraria o job, e `from_json` retornaria
`null` silenciosamente para campos que não batem com o schema declarado —
exatamente o tipo de perda de dado silenciosa que quero evitar. Uso
`get_json_object` para extrair cada campo estável (`id_proposta`,
`operation_type` etc.) como coluna tipada, e deixo os campos heterogêneos
como string JSON intacta. Achatar isso em colunas por tipo de produto,
preservando o resto numa coluna semiestruturada, é o trabalho da Fase 4 —
aqui seria transformação de negócio antecipada.

**Guardo `envelope_bruto` (a mensagem Kafka original) como coluna própria.**
Mesmo já extraindo os campos que preciso, mantenho a string JSON completa e
sem nenhum parsing ao lado. Se algum dia eu descobrir um bug na extração
(um campo mal interpretado, por exemplo), os dados brutos continuam lá — a
tabela bronze nunca perde informação que só existia na minha lógica de
parsing do momento em que rodou.

**Não faço nenhuma deduplicação no bronze.** O conector garante
at-least-once, não exatamente-uma-vez — então o mesmo evento pode
aparecer duas vezes no Kafka em cenários de crash-e-reprocessamento
(documentado na Fase 2). Bronze grava tudo, duplicatas incluídas, porque
decidir "o que conta como o mesmo evento" é uma regra de negócio, e a
regra explícita deste projeto é: nenhuma transformação de negócio no
bronze. A deduplicação acontece no merge da Fase 4.

**Checkpoint da Structured Streaming no MinIO (`s3a://`), não em disco
local do container.** Sem isso, matar o container do job perderia o
progresso do streaming (offsets do Kafka processados até agora) junto com
o container — a próxima subida reprocessaria tudo desde o início do
tópico. Com o checkpoint em `s3a://bronze/_checkpoints/propostas`,
comprovei reiniciando o serviço `bronze`: o offset do checkpoint continuou
de onde parou, sem reprocessar nem pular nada.

**`spark.cores.max=2` explícito no job — descobri isso testando de
verdade, não por antecipação.** O `spark-worker` de nó único tem um total
fixo de cores; sem limitar quanto cada aplicação Spark pode reservar, a
primeira aplicação submetida ao cluster (o próprio job bronze) toma **todo**
o cluster por padrão no modo standalone, e qualquer outra tentativa de
rodar uma segunda aplicação (uma consulta ad-hoc, ou os jobs de silver/gold
que vêm nas próximas fases) fica esperando recursos que nunca sobram. Achei
isso ao tentar rodar uma verificação ad-hoc contra a tabela Delta enquanto
o bronze estava no ar: o comando simplesmente travou. Corrigi limitando
cada job a uma fração do cluster (`spark.cores.max`) e aumentei o
`spark-worker` de 2 para 6 cores, dando espaço para bronze + silver + gold
rodarem ao mesmo tempo mais adiante. Para qualquer consulta ad-hoc contra o
Delta (como fiz para validar isso), uso `--master local[2]` em vez de
`spark://spark-master:7077` — não compete pelo cluster standalone e lê o
Delta no MinIO da mesma forma.

**Trigger de streaming explícito (`BRONZE_TRIGGER_INTERVALO=10 seconds`),
não o default "o mais rápido possível".** Um trigger nomeado deixa claro
que isso é Structured Streaming de verdade (motor de micro-batch nativo do
Spark, com checkpoint e rastreamento de offset), não um `while True` com
`sleep` disfarçado de streaming — e evita gerar um arquivo Parquet novo a
cada evento individual, o que acabaria em excesso de arquivos pequenos no
Delta.

## Decisões e trade-offs (Fase 4)

**Dedupliquei por `resume_token_data`, não por `(id_proposta,
cluster_time_epoch)`.** `resume_token_data` é o `_id` do próprio evento do
change stream do MongoDB — único por definição, já que é a posição exata
no oplog. Duas mensagens Kafka com o mesmo `resume_token_data` são
garantidamente o mesmo evento reprocessado (o cenário at-least-once que a
Fase 2 documentou), não uma coincidência de timing. Usei
`dropDuplicatesWithinWatermark` (recurso do Spark 3.5, feito
especificamente para esse padrão de CDC) em vez do `dropDuplicates` +
`withWatermark` clássico, porque não preciso incluir a coluna de tempo na
chave de deduplicação — só o `resume_token_data` já identifica
univocamente o evento.

**Política explícita para dado atrasado: dentro da janela do watermark, é
deduplicado; fora dela, não é mais deduplicado pelo motor de streaming, mas
não corrompe o `estado_atual`.** Escolhi `SILVER_WATERMARK_ATRASO=10
minutes` como padrão — dá margem folgada para os cenários de reconexão da
Fase 2 (Kafka ou Mongo caindo por alguns minutos) sem manter estado
indefinidamente. Um duplicado que chegasse depois dessa janela deixaria de
ser pego pela deduplicação, mas o `MERGE` do `estado_atual` só aplica
`UPDATE` quando `novo.hora_evento > alvo.hora_evento` — um evento antigo
reaparecendo nunca regride um estado mais novo. **Provei os dois lados dessa
decisão com teste**: `test_duplicata_exata_dentro_do_watermark_e_descartada`
confirma a deduplicação dentro da janela, e
`test_reprocessamento_atrasado_nao_regride_o_estado_atual` confirma que,
mesmo sem a proteção do watermark, o guard de timestamp no merge segura a
consistência.

**Histórico usa fechar-e-inserir em duas etapas, não um único `MERGE`.**
Um `MERGE` do Delta não consegue expressar "insira a linha 1, depois use a
linha 1 para fechar a linha 2" quando várias transições da mesma proposta
caem no mesmo micro-lote (ex.: `em_analise → aprovada → paga` no mesmo
lote). Resolvi em duas operações: primeiro um `MERGE` que só fecha a linha
aberta existente na tabela alvo (usando a primeira transição do lote por
proposta), depois um `append` simples das novas linhas — cada uma já com
seu próprio `valido_ate` calculado via `lead()` dentro do lote. A única
linha que fica com `valido_ate = NULL` é a mais recente por proposta,
tornando-a a "linha aberta" que o próximo lote vai fechar.

**Delete remove do `estado_atual`, mas fecha o histórico com `status =
'deletada'`.** Uma proposta deletada era "criada por engano" (decisão da
Fase 1) — não faz sentido ela continuar aparecendo como se ainda existisse
no estado atual. Mas simplesmente apagar sua história também esconderia
que ela existiu e foi removida. Optei por um meio-termo auditável: sai do
`estado_atual`, mas o histórico ganha uma última linha explícita marcando
o encerramento.

**`campos_especificos_json` é uma string JSON, não um `MAP<STRING,
STRING>`.** Um mapa forçaria todo valor (inteiro, float, string) a virar
texto de qualquer forma — uma coluna JSON preserva os tipos originais
(`prazo_meses` continua inteiro, `taxa_juros` continua float dentro do
JSON) e é igualmente consultável via `get_json_object`/`from_json` sob
demanda, sem exigir um schema Spark rígido que quebraria a cada campo novo
por tipo de produto.

**Descobri rodando de verdade que o custo do `MERGE` cresce com o tamanho
da tabela alvo.** Cada `MERGE INTO` precisa escanear/planejar contra a
tabela inteira; com o `historico`/`estado_atual` crescendo, o tempo por
lote foi subindo (de ~5s para mais de 30s por lote após um restart com
backlog acumulado), gerando avisos de "batch falling behind" no log. Isso
é esperado e não afeta a correção (o motor de streaming absorve o atraso,
não perde nem duplica dado), mas é uma limitação real que documento aqui
em vez de esconder: em produção, isso pediria `OPTIMIZE`/Z-ordering
periódico nas tabelas Delta — fora do escopo desta fase, mas anotado como
próximo passo natural de performance, não de correção.

## Estrutura do repositório

```
infra/            docker-compose.yml, Dockerfile do Spark, scripts de init
generator/        gerador de carga operacional (Fase 1), com generator/tests/
connector/        conector de change streams (Fase 2), com connector/tests/
spark-jobs/       jobs Spark: bronze/ (Fase 3), silver/ (Fase 4); gold/ ainda não implementado
scripts/          bootstrap.sh, teardown.sh, generate-kafka-cluster-id.sh
```

Roteiros de validação manual e de resiliência ficam no próprio README (ver
"Roteiro de validação manual (Fase 1)" e "Roteiro de resiliência
(Fase 2)", acima) — nada de arquivo markdown solto por fase.

## Próximos passos

O que pretendo adicionar na Fase 5, a última:

- Agregações gold em janela (volume/valor por produto, taxa de aprovação,
  tempo médio até decisão, funil de status).
- Checks de qualidade no fluxo (estado impossível, valor fora de faixa,
  transição inválida) e reconciliação entre a contagem no Mongo e no gold.
- Métricas operacionais do pipeline (atraso de consumo, eventos por
  segundo, tamanho do estado) e como acompanhá-las.
- Consultas de exemplo em DuckDB sobre o gold, versionadas no repositório.
- A seção **"Resiliência"** consolidada olhando o pipeline inteiro, e a
  comparação **change streams × CDC do SQL Server** — fazem mais sentido
  como fechamento com o pipeline completo do que forçadas numa fase
  anterior.
- Capturas de tela da Spark UI em streaming, do tópico Kafka e das
  consultas no DuckDB.
