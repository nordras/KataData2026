package com.katadata.config

import java.util.UUID

case class PipelineConfig(
    sourceType: String,
    sqliteDbPath: String,
    sqliteJdbcUrl: String,
    runId: String
)

object PipelineConfig {
  val SourceFile = "file"
  val SourceDb = "db"
  val SourceWs = "ws"
  val DefaultDbPath = "data/analytics.db"

  def fromArgs(args: Array[String]): PipelineConfig = {
    val sourceType = if (args.nonEmpty) args(0).toLowerCase else SourceFile
    val dbPath = if (args.length > 1) args(1) else DefaultDbPath
    val jdbcUrl = s"jdbc:sqlite:$dbPath"
    val runId = UUID.randomUUID().toString

    PipelineConfig(
      sourceType = sourceType,
      sqliteDbPath = dbPath,
      sqliteJdbcUrl = jdbcUrl,
      runId = runId
    )
  }

  def isValidSourceType(sourceType: String): Boolean = {
    sourceType == SourceFile || sourceType == SourceDb || sourceType == SourceWs
  }
}
