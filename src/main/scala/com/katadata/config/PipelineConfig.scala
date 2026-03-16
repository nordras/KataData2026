package com.katadata.config

import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

case class PipelineConfig(
  sourceType: String,
  sqliteDbPath: String,
  wsUrl: String,
  parallelism: Int,
  runId: String
) {
  def sqliteJdbcUrl: String = s"jdbc:sqlite:$sqliteDbPath"
}

object PipelineConfig {

  def fromArgs(args: Array[String]): PipelineConfig = {
    val sourceType = args.lift(0).getOrElse("file").toLowerCase
    val sqliteDbPath = args.lift(1).getOrElse("data/analytics.db")
    val wsUrl = args.lift(2).getOrElse("http://localhost:8090/sales")
    val parallelism = args.lift(3).flatMap(v => scala.util.Try(v.toInt).toOption).getOrElse(2)

    PipelineConfig(
      sourceType = sourceType,
      sqliteDbPath = sqliteDbPath,
      wsUrl = wsUrl,
      parallelism = parallelism,
      runId = "run-" + DateTimeFormatter.ofPattern("yyyyMMddHHmmss").format(LocalDateTime.now())
    )
  }
}
