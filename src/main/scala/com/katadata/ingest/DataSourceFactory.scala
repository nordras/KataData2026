package com.katadata.ingest

object DataSourceFactory {
  def fromSourceType(sourceType: String): DataSource = {
    sourceType.toLowerCase match {
      case "file" => new FileSystemSource
      case "db" => new RelationalDatabaseSource
      case "ws" => new WebServiceSource
      case other => throw new IllegalArgumentException(s"Unsupported source type: $other")
    }
  }
}
