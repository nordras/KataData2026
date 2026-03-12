package com.katadata.ingest

import org.apache.spark.sql.{DataFrame, SparkSession}

trait DataSource {
  def name: String
  def read(spark: SparkSession, runId: String): DataFrame
}

class FileSystemSource extends DataSource {
  override val name: String = "FILE_SYSTEM"

  override def read(spark: SparkSession, runId: String): DataFrame = {
    import spark.implicits._
    import java.sql.Timestamp
    import java.time.Instant

    val now = Timestamp.from(Instant.now())

    Seq(
      ("F-001", "2026-03-10T09:00:00", "Sao Paulo", "SP-01", "Ana Lima", 1200.0, "FILE_SYSTEM", runId, "file", "mock_file_sales.csv", now),
      ("F-002", "2026-03-10T10:10:00", "Sao Paulo", "SP-02", "Bruno Alves", 1850.0, "FILE_SYSTEM", runId, "file", "mock_file_sales.csv", now),
      ("F-003", "2026-03-10T11:15:00", "Rio de Janeiro", "RJ-01", "Carlos Nunes", 980.0, "FILE_SYSTEM", runId, "file", "mock_file_sales.csv", now),
      ("F-004", "2026-03-10T11:50:00", "Curitiba", "CT-01", "Daniela Rocha", 1300.0, "FILE_SYSTEM", runId, "file", "mock_file_sales.csv", now)
    ).toDF(
      "sale_id",
      "sale_ts",
      "city",
      "salesman_id",
      "salesman_name",
      "amount",
      "source_system",
      "ingest_run_id",
      "ingest_source_type",
      "ingest_source_name",
      "ingest_ts"
    )
  }
}

class RelationalDatabaseSource extends DataSource {
  override val name: String = "RELATIONAL_DB"

  override def read(spark: SparkSession, runId: String): DataFrame = {
    import spark.implicits._
    import java.sql.Timestamp
    import java.time.Instant

    val now = Timestamp.from(Instant.now())

    Seq(
      ("D-001", "2026-03-10T09:40:00", "Sao Paulo", "SP-01", "Ana Lima", 2200.0, "RELATIONAL_DB", runId, "db", "mock_sales_table", now),
      ("D-002", "2026-03-10T12:05:00", "Rio de Janeiro", "RJ-02", "Elaine Matos", 1450.0, "RELATIONAL_DB", runId, "db", "mock_sales_table", now),
      ("D-003", "2026-03-10T13:30:00", "Curitiba", "CT-02", "Fabio Souza", 990.0, "RELATIONAL_DB", runId, "db", "mock_sales_table", now),
      ("D-004", "2026-03-10T14:20:00", "Curitiba", "CT-01", "Daniela Rocha", 1900.0, "RELATIONAL_DB", runId, "db", "mock_sales_table", now)
    ).toDF(
      "sale_id",
      "sale_ts",
      "city",
      "salesman_id",
      "salesman_name",
      "amount",
      "source_system",
      "ingest_run_id",
      "ingest_source_type",
      "ingest_source_name",
      "ingest_ts"
    )
  }
}

class WebServiceSource extends DataSource {
  override val name: String = "TRADITIONAL_WS"

  override def read(spark: SparkSession, runId: String): DataFrame = {
    import spark.implicits._
    import java.sql.Timestamp
    import java.time.Instant

    val now = Timestamp.from(Instant.now())

    Seq(
      ("W-001", "2026-03-10T08:35:00", "Sao Paulo", "SP-03", "Gabriela Costa", 1700.0, "TRADITIONAL_WS", runId, "ws", "mock_ws_sales_service", now),
      ("W-002", "2026-03-10T10:45:00", "Rio de Janeiro", "RJ-01", "Carlos Nunes", 2100.0, "TRADITIONAL_WS", runId, "ws", "mock_ws_sales_service", now),
      ("W-003", "2026-03-10T15:10:00", "Rio de Janeiro", "RJ-03", "Helena Pinto", 860.0, "TRADITIONAL_WS", runId, "ws", "mock_ws_sales_service", now),
      ("W-004", "2026-03-10T16:00:00", "Curitiba", "CT-02", "Fabio Souza", 1750.0, "TRADITIONAL_WS", runId, "ws", "mock_ws_sales_service", now)
    ).toDF(
      "sale_id",
      "sale_ts",
      "city",
      "salesman_id",
      "salesman_name",
      "amount",
      "source_system",
      "ingest_run_id",
      "ingest_source_type",
      "ingest_source_name",
      "ingest_ts"
    )
  }
}

object DataSourceFactory {
  def fromSourceType(sourceType: String): DataSource = {
    sourceType.toLowerCase match {
      case "file" => new FileSystemSource()
      case "db" => new RelationalDatabaseSource()
      case "ws" => new WebServiceSource()
      case other => throw new IllegalArgumentException(s"Unsupported source type '$other'. Use: file | db | ws")
    }
  }
}
