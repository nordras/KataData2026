# KataData2026 - Spark + Scala

POC do kata com processamento em Spark (Scala), persistência em SQLite e API mínima.

## Pré-requisitos

- Java 17
- sbt

## 1) Gerar agregados no SQLite

Executa o job Spark e grava no banco `data/analytics.db`:

```powershell
sbt run
```

Tabelas geradas:

- `top_sales_per_city`
- `top_salesman_country`

## 2) Subir API mínima

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
