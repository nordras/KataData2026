# KataData2026 - Spark + Scala

POC do kata com arquitetura simples de warehouse (landing, curated, marts), ingestao mockada e API minima.

## Pre-requisitos

- Java 17
- sbt

## 1) Executar pipeline com ingestao mockada

O job recebe `sourceType` como primeiro argumento:

- `file` para File System mock
- `db` para Relational DB mock
- `ws` para Traditional WS-* mock

Comando padrao (file + caminhos default):

```powershell
sbt run
```

Comandos explicitos por fonte:

```powershell
sbt "runMain com.katadata.jobs.SalesAggregationsJob file data/analytics.db"
sbt "runMain com.katadata.jobs.SalesAggregationsJob db data/analytics.db"
sbt "runMain com.katadata.jobs.SalesAggregationsJob ws data/analytics.db"
```

## Camadas do warehouse

- Landing (SQLite): `landing_sales`
- Curated (SQLite): `curated_sales`
- Marts (SQLite): `top_sales_per_city` e `top_salesman_country`
- Observabilidade minima (SQLite): `pipeline_runs`

## 2) Subir API

Em outro terminal:

```powershell
sbt "runMain com.katadata.api.Server"
```

Parâmetros opcionais:

```powershell
sbt "runMain com.katadata.api.Server data/analytics.db 8080"
```

## Endpoints

- `GET /health`
- `GET /top-sales-per-city`
- `GET /top-salesman-country`
- `GET /pipeline-runs`
