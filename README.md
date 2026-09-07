# Streaming de propostas de crédito — MongoDB, Kafka e Spark

Pipeline de dados que captura mudanças de estado de propostas de crédito
(consignado, cartão e FGTS) gravadas em MongoDB, propaga essas mudanças via
change streams para o Kafka, e as materializa em camadas bronze/silver/gold
sobre Delta Lake, usando Spark Structured Streaming de ponta a ponta.

Este é o quarto projeto do meu portfólio de Engenharia de Dados. Construí
tudo sem nenhum serviço de nuvem pago — todo componente roda em Docker
Compose, na mesma rede.

## Status do projeto

Construí isso em cinco fases, validando cada uma de ponta a ponta antes de
passar para a próxima. Todas estão prontas:

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
- [x] **Fase 5 — Gold, qualidade e observabilidade**: agregações em janela
      (funil de status, volume/valor por produto), tempo de decisão, checks
      de qualidade (estado impossível, valor fora de faixa, transição
      inválida), reconciliação Mongo × gold, consultas de exemplo em DuckDB

A comparação change streams × CDC do SQL Server e a seção "Resiliência"
consolidada (olhando o pipeline inteiro, não só uma peça) ficam para o
final, depois de "Decisões e trade-offs" — fazem mais sentido como
fechamento com o pipeline completo pronto.

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

    subgraph f5["Fase 5 — pronto"]
        GOLD["Spark: agregações em janela\n+ tempo de decisão"]
        QUALIDADE["Spark: checks de qualidade\n(estado, valor, transição)"]
    end
    LAKE_S --> GOLD
    LAKE_B --> QUALIDADE
    LAKE_S --> QUALIDADE
    GOLD -->|Delta| LAKE_G[("MinIO: gold")]
    QUALIDADE -->|Delta| LAKE_G
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

## Fase 5 — o que existe hoje

### Gold: métricas de negócio e tempo de decisão

Escrevi `spark-jobs/gold/gold_job.py`, um job Spark Structured Streaming com
três streaming queries rodando na mesma `SparkSession`
(`spark.streams.awaitAnyTermination()`), porque cada uma tem uma fonte ou
uma semântica de agregação diferente:

- **`s3a://gold/funil_status`**: agregação em janela (`GOLD_JANELA_FUNIL`,
  padrão 1 dia) por `tipo_produto` + `status`, lendo o histórico da Fase 4
  como stream. Uso `groupBy(window(...), tipo_produto, status).count()` com
  `outputMode("update")` — cada micro-lote emite o total **já acumulado**
  daquela janela até agora, não um delta, então o `MERGE` no `foreachBatch`
  substitui a quantidade persistida em vez de somar.
- **`s3a://gold/volume_valor_por_produto`**: mesma técnica de janela, mas lida
  direto do bronze e filtra só `operation_type = 'insert'` — cada proposta
  entra exatamente uma vez nessa contagem, então atualizações posteriores de
  status não inflam volume nem valor.
- **`s3a://gold/tempo_decisao`**: uma linha por proposta que chegou a
  `aprovada`/`recusada`, com o tempo entre a primeira entrada no histórico e
  a decisão. Calculado via releitura estática do histórico completo dentro
  do `foreachBatch` (mesma técnica que a Fase 4 já usa para o `MERGE` do
  `estado_atual`), buscando o `min(valido_de)` por `id_proposta`.
- **`s3a://gold/contagem_atual_por_status`**: recomputada por inteiro
  (`overwrite`) a cada micro-lote, contando linhas do histórico com
  `valido_ate IS NULL` e `status != 'deletada'`. É o número que uso na
  reconciliação com o Mongo, abaixo.

### Qualidade: estado impossível, valor fora de faixa, transição inválida

Escrevi `spark-jobs/gold/qualidade_job.py` separado do `gold_job.py` porque
lida com fontes e uma finalidade diferentes (validação, não métrica de
negócio). Roda dois checks independentes, cada um sua própria streaming
query, escrevendo violações em `s3a://gold/qualidade_violacoes`:

- **Estado impossível** e **valor fora de faixa**: lidos do bronze (o
  documento bruto de `insert`/`update`/`replace`), comparando `status`
  contra o conjunto de estados conhecidos e `valor_solicitado` contra a
  faixa observada de cada produto — os mesmos limites que o próprio gerador
  de carga usa em `generator/app/factories.py` (`spark-jobs/gold/transformacoes.py`,
  `FAIXA_VALOR_POR_PRODUTO`). Deletes não são verificados: o objetivo é
  sinalizar dado inválido entrando no pipeline, não reauditar o que já saiu
  dele.
- **Transição inválida**: lida do histórico, comparando o status de cada
  linha nova com o status da linha imediatamente anterior da mesma
  proposta — usando `lag("status")` sobre uma janela ordenada por
  `(valido_de, resume_token_data)`, o mesmo critério de desempate que a
  Fase 4 já usa para decidir ordem dentro de um lote. Comparo contra o
  mesmo grafo de transições de `generator/app/state_machine.py`
  (duplicado deliberadamente em `transformacoes.py`: o job gold não
  depende do pacote do gerador para continuar implantável de forma
  independente — mantido em sincronia manualmente, um trade-off que
  registro em vez de esconder).

| Variável | Papel |
|---|---|
| `GOLD_FUNIL_PATH` / `GOLD_VOLUME_VALOR_PATH` / `GOLD_TEMPO_DECISAO_PATH` / `GOLD_CONTAGEM_ATUAL_PATH` / `GOLD_VIOLACOES_PATH` | caminho de cada tabela Delta de saída |
| `GOLD_CHECKPOINT_*` | um checkpoint por streaming query (5 no total entre os dois jobs) |
| `GOLD_WATERMARK_ATRASO` / `GOLD_JANELA_FUNIL` | mesma lógica de tolerância a atraso da Fase 4, aplicada às agregações em janela |

### Reconciliação Mongo × gold

`contagem_atual_por_status` soma para o total de propostas que o pipeline
considera "abertas" (não deletadas). Esse número tem que bater com a
contagem de documentos no MongoDB — são, por definição, a mesma coisa vista
por dois caminhos diferentes. Com o gerador parado (para a contagem não
mudar no meio da comparação):

```bash
docker compose -f infra/docker-compose.yml --env-file infra/.env \
  exec mongo mongosh --quiet creditodb --eval "db.propostas.countDocuments({})"
```

E do lado do gold, via DuckDB (`queries/gold/reconciliacao_contagem_atual.sql`,
conexão em `queries/gold/00_conexao.sql`):

```sql
SELECT sum(quantidade) AS total_propostas_abertas_no_gold
FROM delta_scan('s3://gold/contagem_atual_por_status');
```

Rodei essa reconciliação de verdade parando `generator`+`connector`, esperando
o pipeline drenar (`bronze`/`silver`/`gold` sem `falling behind` no log) e
comparando os dois números — bateram exatamente. Enquanto o gerador está
ativo, um descompasso pequeno é esperado (eventos em trânsito no Kafka ou
ainda não processados pelo streaming) — não é sinal de perda de dado, só de
latência de ponta a ponta.

### O que a validação de qualidade encontrou rodando de verdade

Rodei o `qualidade_job.py` contra o backlog acumulado de várias horas do
gerador de carga e ele apontou **508 transições "inválidas"** — um número
alto demais para ignorar. Investigando os pares de status mais comuns
(`em_analise -> paga`, `paga -> em_analise`, `cancelada -> aprovada`...), a
causa raiz não estava na Fase 5: era uma corrida de condição real e bem
documentada do `fullDocument: "updateLookup"` do MongoDB — ele busca o
documento **no momento em que o change stream é lido**, não no momento em
que aquele update específico aconteceu. Duas atualizações rápidas em
sequência na mesma proposta (ex.: `aprovada` seguida de `paga` poucos
segundos depois) podem fazer o evento mais antigo chegar com
`full_document.status` já mostrando o valor **mais novo**, mesmo
`updateDescription.updatedFields.status` trazendo o valor correto daquele
evento específico. Confirmei isso direto no bronze: o evento com
`updatedFields: {"status": "aprovada"}` trazia `full_document.status =
"paga"`.

Esse é um bug real na Fase 4, não na Fase 5 — `estado_atual.status` também
ficava sujeito a mostrar um valor adiantado nesse cenário. Corrigi em
`spark-jobs/silver/transformacoes.py` (`mesclar_full_document_com_update_description`):
para eventos de `update`, sobreponho `updateDescription.updatedFields` em
cima do `full_document` antes de extrair qualquer campo — o que aquele
evento especificamente mudou sempre vence a leitura potencialmente
adiantada. Reprocessei bronze → silver → gold do zero (limpando checkpoints
e tabelas) e as transições inválidas caíram de 508 para 53 e, depois de
também corrigir a ausência de `(*, "deletada")` no grafo de transições
válidas (delete pode acontecer a partir de qualquer estado, por decisão da
Fase 1), para **2 ocorrências** num total de milhares de transições.

Não persegui essas 2 últimas até a raiz — a hipótese mais provável é uma
interação rara entre o at-least-once do conector (Fase 2) e o timing exato
de reconexões, mas não confirmei. Deixo isso registrado como um resíduo
conhecido, não escondido: o check de qualidade está fazendo exatamente o
que deveria — apontar anomalias reais para investigação, não fingir que o
pipeline é perfeito.

### Bug de escala encontrado rodando de verdade: `.isin()` com lista coletada no driver

A primeira versão do `checar_transicoes` e do `escrever_tempo_decisao`
coletava os `id_proposta` do micro-lote pro driver (`.collect()`) e filtrava
a leitura estática do histórico com `.filter(col("id_proposta").isin(lista))`.
Funciona bem com poucos IDs por lote — mas ao reprocessar um backlog de
horas depois de eu ter zerado um checkpoint de teste, o job **travou**
silenciosamente (sem erro, sem exceção, CPU do executor parado) processando
um único micro-lote com milhares de IDs distintos: embutir uma lista desse
tamanho no plano lógico do Catalyst explode o tamanho do plano serializado
(vi o aviso `Broadcasting large task binary with size 2.2 MiB` no log antes
de travar). Troquei por um `join` com `F.broadcast()` contra um DataFrame de
IDs distintos — a mesma filtragem, mas o Spark resolve via hash join em vez
de uma cláusula `IN` gigante no plano, e escala independente do tamanho do
backlog.

### Testando de verdade a Fase 5: pytest local + streaming real via Docker

`spark-jobs/gold/transformacoes.py` reúne a lógica pura e testável sem Spark
(`estado_e_impossivel`, `valor_fora_de_faixa`, `transicao_e_valida`):

```bash
cd spark-jobs
python -m venv .venv && source .venv/bin/activate
pip install -r requirements-dev.txt
pytest gold/tests/test_transformacoes.py -q
```

`gold/tests/test_gold_job.py` e `gold/tests/test_qualidade_job.py` sobem uma
`SparkSession` local dentro da imagem `credpipe/spark`, como já fazia a
Fase 4:

```bash
docker run --rm --user root \
  -v "<caminho-absoluto-do-repo>/spark-jobs:/work" \
  -e HOME=/tmp \
  -e PYTHONPATH="/opt/spark/python:/opt/spark/python/lib/py4j-0.10.9.7-src.zip:/opt/spark/python/lib/pyspark.zip" \
  -w /work/gold \
  credpipe/spark:3.5.8 sh -c "pip3 install -q pytest && python3 -m pytest tests/ -q"
```

Os testes provam, com Spark de verdade: que a agregação de janela soma
corretamente eventos no mesmo grupo e não atualiza uma janela já expirada
pelo watermark (`test_evento_muito_atrasado_nao_atualiza_janela_ja_expirada`);
que `tempo_decisao` calcula o intervalo certo; que `contagem_atual_por_status`
reflete só propostas ainda abertas; que um documento com status desconhecido
ou valor fora da faixa do produto gera violação e um documento válido não
gera nenhuma; que delete não é verificado; e que duas transições da mesma
proposta no **mesmo segundo** (a colisão de precisão que o
`cluster_time_epoch` com granularidade de segundo pode causar) são
corretamente ordenadas usando `resume_token_data` como desempate, sem gerar
falso positivo (`test_transicoes_no_mesmo_segundo_usam_resume_token_como_desempate`).

### Observabilidade

- **Spark UI por serviço**: `bronze` (4040), `silver` (4041), `gold` (4042),
  `qualidade` (4043) — cada um com sua própria aba "Structured Streaming",
  mostrando taxa de entrada/processamento por query e, para o `funil_status`
  (a única agregação com estado deste projeto), o gráfico "Aggregated
  Number Of Total State Rows" — é ali que se vê o tamanho do estado
  crescendo com o número de janelas × produtos × status ainda dentro do
  watermark.
- **Atraso de consumo do Kafka**: não uso `kafka-consumer-groups.sh
  --describe` para medir lag do `bronze` — o conector de leitura Kafka do
  Spark Structured Streaming **não comita offset como grupo de consumidor**
  (gerencia tudo via checkpoint próprio), então essa ferramenta clássica não
  mostra nada útil aqui. O sinal correto é o progresso da própria query, via
  Spark UI ou `query.lastProgress`: cada `StreamingQueryProgress` traz, por
  fonte, `latestOffset` (o que já existe no tópico) e `endOffset` (até onde
  este micro-lote leu) — a diferença entre os dois é o atraso real.
- **Eventos por segundo**: visível tanto no log do `generator`
  (`docker compose logs -f generator`) quanto no gráfico "Input Rate" de
  cada streaming query no Spark UI.

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

**`spark.cores.max` explícito no job — descobri isso testando de verdade,
não por antecipação.** O `spark-worker` de nó único tem um total fixo de
cores; sem limitar quanto cada aplicação Spark pode reservar, a primeira
aplicação submetida ao cluster (o próprio job bronze) toma **todo** o
cluster por padrão no modo standalone, e qualquer outra tentativa de rodar
uma segunda aplicação (uma consulta ad-hoc, ou os jobs de silver/gold que
vêm nas próximas fases) fica esperando recursos que nunca sobram. Achei isso
ao tentar rodar uma verificação ad-hoc contra a tabela Delta enquanto o
bronze estava no ar: o comando simplesmente travou. Corrigi limitando cada
job a uma fração do cluster (`spark.cores.max=2` nesta fase, aumentei o
`spark-worker` de 2 para 6 cores) para dar espaço a bronze + silver + gold
rodarem ao mesmo tempo mais adiante — a Fase 5, ao somar um quarto job
concorrente (`qualidade`) e esbarrar no limite de memória da VM do WSL2,
reduziu esses números outra vez (`spark.cores.max=1` por job,
`spark-worker` com 4 cores; ver "Resiliência", abaixo). Para qualquer
consulta ad-hoc contra o Delta (como fiz para validar isso), uso
`--master local[2]` em vez de `spark://spark-master:7077` — não compete
pelo cluster standalone e lê o Delta no MinIO da mesma forma.

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

## Decisões e trade-offs (Fase 5)

**Separei `gold_job.py` de `qualidade_job.py` em vez de um job único.**
Métrica de negócio (funil, volume/valor, tempo de decisão) e checagem de
qualidade têm fontes e finalidades diferentes — `qualidade_job.py` lê bronze
diretamente (documento bruto, antes de qualquer achatamento), enquanto
`gold_job.py` lê exclusivamente o histórico já processado da Fase 4. Juntar
os dois num job só significaria misturar dois conjuntos de streaming
queries com propósitos diferentes na mesma `SparkSession`, sem ganho real —
cada um já roda isolado em seu próprio serviço Compose, com seu próprio
Spark UI.

**Escolhi `outputMode("update")` + `MERGE` de substituição para as
agregações em janela, não `outputMode("append")`.** Com `append`, o Spark só
emite uma janela quando ela é considerada definitivamente fechada pelo
watermark — o que atrasaria a visibilidade dos números por até
`GOLD_WATERMARK_ATRASO`. Com `update`, cada micro-lote já emite o total
corrente da janela (ainda pode receber mais eventos depois, dentro do
watermark), então o `MERGE` no `foreachBatch` **substitui** a quantidade
persistida em vez de somar — se fosse `append` incremental, um mesmo
micro-lote reprocessado (ex.: depois de um restart) duplicaria a contagem.

**Faixas de valor por produto vieram do próprio gerador de carga, não de
uma regra de negócio nova.** `FAIXA_VALOR_POR_PRODUTO` em
`spark-jobs/gold/transformacoes.py` usa os mesmos intervalos de
`generator/app/factories.py` (consignado 1.000–50.000, cartão 500–15.000,
fgts 1.000–27.000). Isso faz o check de qualidade pegar um caso real e
esperado: uma correção retroativa (que sorteia um novo valor entre 500 e
50.000, sem respeitar o teto do produto) pode empurrar uma proposta de
cartão para um valor acima do seu teto — validado rodando de verdade contra
o gerador, não um cenário sintético.

**Prefiro dois testes de integração reproduzindo colisões de precisão a
confiar cegamente no `resume_token_data` como desempate.** O
`cluster_time_epoch` (desde a Fase 3) tem granularidade de segundo — duas
transições da mesma proposta no mesmo segundo são um cenário real sob a
carga deste gerador, não hipotético. Optei por **não** aumentar a precisão
do timestamp agora (mudaria o schema do bronze, uma fase já revisada);
em vez disso, todo código da Fase 5 que precisa de ordem usa
`(valido_de, resume_token_data)` como chave de ordenação — o mesmo critério
que a Fase 4 já usa — e provei com teste que isso resolve corretamente o
empate.

## Resiliência

Consolidando aqui os cenários testados de verdade ao longo de todas as
fases, mais um novo, olhando o pipeline inteiro:

**Reinício de qualquer job Spark (bronze/silver/gold/qualidade) retoma do
checkpoint, sem perder nem duplicar.** Testado reiniciando cada serviço
individualmente com `docker compose restart <serviço>` durante carga ativa:
o offset do checkpoint sempre continuou de onde parou.

**Queda do Kafka ou do MongoDB é absorvida pelo conector com backoff
exponencial, sem perder evento.** Roteiro completo na Fase 2, acima
("Roteiro de resiliência (Fase 2)") — o pior caso é reprocessar o último
lote em memória, nunca perder um.

**Resume token inválido/corrompido faz o conector falhar de forma visível
(`docker compose ps` mostra `Exited`), nunca pular eventos silenciosamente.**
Também documentado na Fase 2.

**Um job downstream (gold/qualidade) não trava a montante se ficar
indisponível — só atrasa.** Parei `gold` e `qualidade` por período extenso
enquanto validava outras partes do pipeline; `bronze` e `silver` continuaram
publicando normalmente (são streams independentes, sem acoplamento direto),
e ao religar `gold`/`qualidade` eles reprocessaram o backlog acumulado do
zero (a partir de seus próprios checkpoints) sem intervenção manual.

**Reprocessar um backlog grande depois de resetar um checkpoint é uma
operação pesada, não instantânea — e pode esbarrar em limites do
ambiente, não do código.** Ao limpar o checkpoint do `qualidade_job.py`
para validar uma correção, o primeiro micro-lote teve que processar horas
de histórico acumulado de uma vez. Isso expôs dois problemas reais:

1. Um bug de escala no meu próprio código (`.isin()` com lista coletada no
   driver não escala — corrigido trocando por `join` com `broadcast`,
   detalhado na Fase 5 acima).
2. Rodar `bronze` + `silver` + `gold` + `qualidade` simultaneamente, cada
   um sua própria JVM Spark, mais Mongo/Kafka/MinIO, pressiona o limite
   padrão da VM do WSL2 (50% da RAM do host, sem `.wslconfig`
   customizado) — cheguei a saturar a VM ao ponto do Docker Desktop pausar
   automaticamente todos os containers (`Resource Saver`). Resolvi
   reduzindo `spark.driver.memory`/`spark.executor.memory` para 512m e
   `spark.cores.max=1` em cada job (de 2), e o `spark-worker` de 8 para 4
   cores — o suficiente para os quatro jobs conviverem em regime normal.
   Para reprocessar um backlog muito grande num ambiente com pouca RAM
   livre, a alternativa mais simples é parar os demais jobs, deixar um
   reprocessar sozinho, e religar os outros em seguida — validei esse
   fluxo na prática.

**MongoDB `fullDocument: "updateLookup"` pode retornar um documento mais
adiantado do que o evento representa, se a proposta sofrer outra escrita
antes do conector ler aquele evento específico.** Descoberto pelo próprio
check de qualidade da Fase 5 (transições "impossíveis" que na verdade eram
leitura racional de um `full_document` desatualizado-para-frente). Corrigido
na Fase 4 (`mesclar_full_document_com_update_description`) sobrepondo
`updateDescription.updatedFields` — o que aquele evento especificamente
mudou — por cima do `full_document`, em vez de confiar cegamente nele.
Detalhado acima, na seção da Fase 5.

## MongoDB change streams × CDC do SQL Server

Já construí captura de mudanças com SQL Server CDC num projeto anterior do
portfólio (`sqlserver-cdc-to-dw`), o que deixa a comparação concreta, não
teórica:

| | MongoDB change streams | SQL Server CDC |
|---|---|---|
| **Mecanismo de captura** | Lê o oplog do replica set diretamente via um cursor tailable (`watch()`) | Job de captura assíncrono lê o transaction log e grava em tabelas de mudança (`cdc.<schema>_<tabela>_CT`) |
| **Latência** | Near real-time — o evento aparece no cursor assim que commitado no oplog | Depende do intervalo de polling do job de captura (segundos, configurável) |
| **Pré-requisito de infraestrutura** | Replica set (mesmo de nó único) — não funciona em standalone | Habilitar CDC no banco e por tabela (`sys.sp_cdc_enable_db`/`_table`); exige Agente SQL Server rodando |
| **Ponto de retomada** | Resume token opaco, atrelado à posição exata no oplog | LSN (`__$start_lsn`) — comparável entre tabelas, consultável diretamente |
| **O que acontece se o consumidor ficar off por muito tempo** | Resume token expira quando o oplog "gira" (rotaciona) — falha explícita e auditável (`InvalidResumeToken`/`ChangeStreamHistoryLost`), como implementei na Fase 2 | Job de limpeza do CDC também expira mudanças antigas (`retention`, padrão 3 dias) — mesmo tipo de falha, mecanismo de expiração diferente |
| **Imagem antes da mudança** | Opt-in por coleção (`changeStreamPreAndPostImages`), com custo de armazenamento extra por gravação | Nativo nas tabelas de mudança (`net_changes` ou `all_changes`, direto na captura) |
| **Granularidade de escuta** | Por coleção, banco ou cluster inteiro (um único `watch()`) | Por tabela — precisa de uma instância CDC por tabela monitorada |
| **Schema heterogêneo** | Change stream não impõe schema — o documento pode variar livremente entre eventos, como explorei nas Fases 3/4 | CDC herda o schema fixo da tabela relacional de origem — heterogeneidade exigiria modelagem prévia (colunas esparsas, ou EAV) |
| **Ordenação garantida** | Por documento, via posição no oplog — usei isso para a chave do Kafka (`id_proposta`) | Por linha, via LSN — mesma garantia, mecanismo diferente |

A diferença que mais pesou na prática: change streams me deram um
mecanismo de captura já pronto para uso (só chamar `watch()`), enquanto CDC
no SQL Server exige provisionar e manter uma instância de captura por
tabela. Em compensação, o LSN do SQL Server é um número comparável e
consultável a qualquer momento — o resume token do Mongo é opaco, só serve
para retomar o mesmo cursor, o que tornou necessário eu mesmo desenhar a
persistência e o tratamento de falha do checkpoint (Fase 2), algo que o
CDC do SQL Server resolve de forma mais direta com a própria coluna de LSN.

## Estrutura do repositório

```
infra/            docker-compose.yml, Dockerfile do Spark, scripts de init
generator/        gerador de carga operacional (Fase 1), com generator/tests/
connector/        conector de change streams (Fase 2), com connector/tests/
spark-jobs/       jobs Spark: bronze/ (Fase 3), silver/ (Fase 4), gold/ (Fase 5, com gold_job.py e qualidade_job.py)
queries/gold/     consultas de exemplo em DuckDB sobre as tabelas gold
scripts/          bootstrap.sh, teardown.sh, generate-kafka-cluster-id.sh
```

Roteiros de validação manual e de resiliência ficam no próprio README (ver
"Roteiro de validação manual (Fase 1)" e "Roteiro de resiliência
(Fase 2)", acima) — nada de arquivo markdown solto por fase.

## Considerações finais

As cinco fases planejadas estão implementadas e validadas rodando de
verdade, não só por teste unitário: MongoDB → Kafka → Spark Structured
Streaming → Delta Lake (bronze/silver/gold) → DuckDB, com resume token
durável, deduplicação por watermark, SCD2, checks de qualidade e
reconciliação Mongo × gold.

O que ainda deixo anotado como próximo passo natural, não como pendência de
correção:

- `OPTIMIZE`/Z-ordering periódico nas tabelas Delta que mais crescem
  (`historico`, `estado_atual`) — o custo do `MERGE` sobe com o tamanho da
  tabela alvo (Fase 4), algo que só compensa resolver quando o volume real
  justificar.
- As duas transições "impossíveis" residuais que o check de qualidade ainda
  aponta depois da correção do `updateLookup` (Fase 5, acima) — candidatas a
  uma investigação futura, não um bug conhecido e ignorado.
- Capturas de tela da Spark UI em streaming, do tópico Kafka e das consultas
  no DuckDB.
