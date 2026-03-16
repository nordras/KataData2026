# Spark vs Flink — Trade-offs para o KataData2026

> Contexto: este documento compara os dois motores **considerando o pipeline atual** (batch, SQLite, API HTTP separada) e orienta a decisão de refazer o projeto com Apache Flink.

---

## 1. O que há por trás do motor

### Apache Spark (atual)

```
Driver (JVM)
  ├── SparkContext → negocia recursos com o cluster manager
  ├── DAGScheduler  → converte operações em estágios (Stage)
  ├── TaskScheduler → distribui Tasks para os Executores
  └── Executors (JVMs separados, ou mesma JVM em local mode)
        ├── cache de partições RDD/DataFrame em memória ou disco
        └── executam Tasks em threads paralelas
```

- **Modelo de execução:** pull-based, micro-batch orientado. O Driver monta o DAG completo **antes** de executar qualquer coisa.
- **Agendamento:** baseado em _stages_ — barreiras de shuffle obrigam todos os Tasks de um stage a terminar antes do próximo começar.
- **Tolerância a falhas:** linhagem (lineage) de RDD / checkpoint de DataFrame. Recomputação desde o último checkpoint.
- **Memória:** Tungsten (codegen binário off-heap) + gestão automática de spill para disco.
- **SQL / DataFrame:** Catalyst Optimizer → Logical Plan → Physical Plan → Código Java gerado em runtime (Whole-Stage CodeGen).

No projeto atual rodamos no modo **local** (`local[*]`), onde Driver e Executors vivem no mesmo processo. É a configuração mais simples mas elimina resiliência distribuída.

---

### Apache Flink

```
JobManager (JVM)
  ├── JobMaster      → ciclo de vida do Job (1 por job)
  ├── ResourceManager → gerencia Task Slots
  ├── Dispatcher     → REST API de submissão
  └── CheckpointCoordinator → orquestra snapshots

TaskManagers (JVMs separados, ou mesmo processo em local mode)
  └── Task Slots
        └── Operators encadeados (chained) em Threads
```

- **Modelo de execução:** push-based, pipeline verdadeiro. Dados fluem de operador em operador **sem barreiras de stage**—o Record sai do Source e atravessa todo o grafo antes de outro chegar.
- **Agendamento:** baseado em _slots_. Operadores compatíveis são encadeados (operator chaining) na mesma thread → zero serialização entre eles.
- **Tolerância a falhas:** Chandy-Lamport distributed snapshots (checkpoints). Em modo batch, usa restart por task; em streaming, restaura todo o job para o último snapshot.
- **Memória:** MemoryManager off-heap próprio + RocksDB como state backend opcional (evita GC pressure em estado grande).
- **SQL / Table API:** Calcite Optimizer → Logical Plan → Physical Plan; em batch suporta hash-join e sort-merge-join nativos.

O modo **BATCH** do Flink (desde 1.12, estável desde 1.14) usa o mesmo runtime de streaming mas com semântica de bounded stream — o papel do Flink como motor batch passou a ser competitivo com o Spark.

---

## 2. Tabela de trade-offs diretos

| Dimensão | Spark 3.5 (atual) | Flink 1.19 (proposto) |
|---|---|---|
| **Modelo primário** | Batch (micro-batch em streaming) | Streaming first, batch como caso especial |
| **Latência batch** | Segundos a minutos (DAG + shuffles) | Sub-segundo a poucos segundos (pipeline) |
| **Facilidade de setup** | ★★★★☆ — `local[*]` funciona sem infra | ★★★☆☆ — precisa de mini-cluster ou modo embedded |
| **API Scala batch** | DataFrame / Spark SQL maduro | Table API + SQL (Calcite) ou DataStream API |
| **Window functions** (rank por cidade) | `Window.partitionBy().orderBy()` nativo | `OVER (PARTITION BY ... ORDER BY ...)` via SQL ou `KeyedProcessFunction` |
| **State management** | Sem estado explícito (batch puro) | Managed state (ValueState, ListState) para streaming; desnecessário em batch |
| **Checkpointing** | Linhagem RDD | Chandy-Lamport snapshots (streaming); irrelevante em batch sem estado |
| **Dependências JAR** | ~300 MB (spark-core + spark-sql) | ~60 MB (flink-clients + flink-table-api-scala) |
| **Startup time local** | 3–8 s (SparkContext + Hive metastore scan) | < 1 s (mini cluster embedded) |
| **Integração com Kafka** | Spark Structured Streaming | Flink Kafka Connector (exactly-once nativo) |
| **Maturidade ecossistema** | ★★★★★ — 10+ anos, documentação extensa | ★★★★☆ — 8+ anos, dominante em streaming |
| **Curva de aprendizado** | Menor para batch SQL/DataFrame | Maior para streaming (estado, watermarks, timers) |
| **JDBC Sink** | `df.write.jdbc(...)` (Spark DataFrameWriter) | `JdbcSink.sink(...)` (Flink connector) |
| **Compatibilidade Java 17** | Requer `--add-opens` (vide build.sbt) | Funciona sem flags extras desde 1.18 |

---

## 3. Por que o Kata original aceita os dois

O brainstorm cita: *"Modern Processing with Spark, Flink or Kafka Streams"*.

| Requisito do Kata | Spark serve? | Flink serve? |
|---|---|---|
| 3 fontes de ingestão (DB, File, WS) | ✅ | ✅ |
| Top Sales per City (window) | ✅ | ✅ |
| Top Salesman Country (aggregation) | ✅ | ✅ |
| Data Lineage | ✅ parcial (colunas `ingest_*`) | ✅ parcial (mesmo padrão) |
| Observabilidade (pipeline_runs) | ✅ | ✅ |
| DB dedicado + API REST | ✅ SQLite + com.sun.httpserver | ✅ mesmo padrão |
| Sem Python, sem Redshift, sem Hadoop | ✅ | ✅ |

Ambos cobrem 100% dos requisitos. A diferença é **como** cobrem e o que você aprende no processo.

---

## 4. Quando escolher Flink neste contexto

**Escolha Flink se:**
- Quer aprender o **runtime de streaming** (watermarks, event time, checkpoints) — o conhecimento é mais transferível para sistemas modernos (Kafka, CDC, Kinesis).
- Quer tempo de startup menor durante o desenvolvimento iterativo.
- Quer explorar o **modo batch nativo** do Flink — bom contraste pedagógico (mesmo código, dois modos de execução).
- O projeto crescer para ingestão em tempo real (substitui Kafka Streams com mais poder).

**Fique com Spark se:**
- O foco é explorar **Catalyst optimizer, Adaptive Query Execution, Delta Lake / Iceberg** no futuro.
- O time conhece SQL e quer o menor atrito possível.
- O volume de dados é grande e você quer benchmarks comparáveis com a indústria.

---

## 5. Mapa de reescrita arquivo a arquivo

| Arquivo atual (Spark) | Equivalente Flink | Esforço |
|---|---|---|
| `build.sbt` | Trocar `spark-*` por `flink-scala`, `flink-table-api-scala`, `flink-connector-jdbc` | Baixo |
| `SalesAggregationsJob.scala` | Substituir `SparkSession` por `StreamExecutionEnvironment` + `TableEnvironment` (modo BATCH) | Médio |
| `SalesTransformations.scala` | Mesmas queries SQL via `tableEnv.sqlQuery(...)` — praticamente 1-para-1 | Baixo |
| `DataSource.scala` (trait) | Trocar assinatura de `DataFrame` para `Table` ou `DataStream[Row]` | Baixo |
| `FileSystemSource.scala` | `tableEnv.executeSql("CREATE TABLE ... WITH ('connector'='filesystem',...)")` | Médio |
| `SalesRepository.scala` | `JdbcSink` + `StatementBuilder` para writes; schema DDL via JDBC direto | Médio |
| `SalesQueryRepository.scala` | Sem mudança — lê SQLite direto via JDBC (sem Flink envolvido) | Nenhum |
| `Server.scala` + handlers | Sem mudança — API HTTP é independente do motor | Nenhum |
| `PipelineConfig.scala` | Adicionar parâmetro de paralelismo | Baixo |

**Estimativa de esforço total:** ~4–6 arquivos alterados, ~200–300 linhas trocadas.

---

## 6. Estrutura de dependências proposta (build.sbt)

```scala
val flinkVersion = "1.19.1"

libraryDependencies ++= Seq(
  // Runtime Flink em modo local (embedded mini-cluster)
  "org.apache.flink" %% "flink-scala"                 % flinkVersion,
  "org.apache.flink" %  "flink-clients"               % flinkVersion,
  // Table API + SQL (substitui Spark SQL)
  "org.apache.flink" %% "flink-table-api-scala-bridge" % flinkVersion,
  "org.apache.flink" %  "flink-table-runtime"          % flinkVersion,
  "org.apache.flink" %  "flink-table-planner-loader"   % flinkVersion,
  // JDBC Sink (substitui df.write.jdbc)
  "org.apache.flink" %  "flink-connector-jdbc"         % "3.2.0",
  // SQLite (sem mudança)
  "org.xerial"       %  "sqlite-jdbc"                  % "3.45.1.0"
)
```

> **Atenção de compatibilidade:** Flink 1.19 requer Scala 2.12. O projeto já está em 2.12.18 — sem mudança de versão de linguagem.

---

## 7. Esqueleto do novo job (Flink Table API, modo BATCH)

```scala
import org.apache.flink.api.common.RuntimeExecutionMode
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.table.api.bridge.scala.StreamTableEnvironment
import org.apache.flink.table.api.EnvironmentSettings

object SalesAggregationsJob {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setRuntimeMode(RuntimeExecutionMode.BATCH)   // modo batch explícito

    val settings = EnvironmentSettings.newInstance().inBatchMode().build()
    val tableEnv = StreamTableEnvironment.create(env, settings)

    // Registra fonte (equivalente ao DataSource.read())
    tableEnv.executeSql(
      """CREATE TEMPORARY TABLE landing_source (
        |  sale_id STRING, sale_ts STRING, city STRING,
        |  salesman_id STRING, salesman_name STRING, amount DOUBLE,
        |  source_system STRING
        |) WITH (
        |  'connector' = 'filesystem',
        |  'path'      = 'src/main/resources/sample_sales.csv',
        |  'format'    = 'csv'
        |)""".stripMargin
    )

    // Equivalente ao calculateTopSalesPerCity()
    val topCity = tableEnv.sqlQuery(
      """SELECT city, salesman_name, total_sales
        |FROM (
        |  SELECT city, salesman_name,
        |         SUM(amount) AS total_sales,
        |         RANK() OVER (PARTITION BY city ORDER BY SUM(amount) DESC) AS rnk
        |  FROM landing_source
        |  GROUP BY city, salesman_name
        |) WHERE rnk = 1""".stripMargin
    )

    // Sink via JdbcSink (equivalente ao repository.writeTopSalesPerCity())
    // tableEnv.createTemporaryView("top_city_result", topCity)
    // ... definir sink connector JDBC para SQLite
  }
}
```

---

## 8. Conclusão

| Critério | Decisão |
|---|---|
| **Prototype funcional** que cobre 100% do Kata | ✅ Flink viável |
| **Batch simples** sem streaming real | Flink modo BATCH — overhead mínimo |
| **Aprendizado do motor** | Flink expõe mais os conceitos (slots, operators, checkpoints) |
| **Risco de reescrita** | Baixo — 50% dos arquivos não mudam (API, handlers, config) |
| **Recomendação** | **Reescrever o job + datasources + repository; manter API HTTP intacta** |

O próximo passo concreto é substituir as dependências no `build.sbt`, adaptar `SalesAggregationsJob` para `TableEnvironment` em modo BATCH, e redirecionar as transformações (que já são SQL puro) para `tableEnv.sqlQuery()`.
