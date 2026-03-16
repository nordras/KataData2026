package com.katadata.ingest

import com.katadata.model.Sale

import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Paths}
import java.time.Instant
import scala.collection.JavaConverters._

object CsvSupport {

  def readSalesFromCsv(path: String, sourceType: String, sourceName: String, runId: String): Seq[Sale] = {
    val filePath = Paths.get(path)
    if (!Files.exists(filePath)) {
      return Seq.empty
    }

    val now = Instant.now().toString
    Files.readAllLines(filePath, StandardCharsets.UTF_8).asScala.toSeq
      .drop(1)
      .filter(_.trim.nonEmpty)
      .flatMap { line =>
        val cols = line.split(",", -1).map(_.trim)
        if (cols.length < 7) {
          None
        } else {
          Some(
            Sale(
              sale_id = cols(0),
              sale_ts = cols(1),
              city = cols(2),
              salesman_id = cols(3),
              salesman_name = cols(4),
              amount = scala.util.Try(cols(5).toDouble).getOrElse(0.0d),
              source_system = cols(6),
              ingest_run_id = runId,
              ingest_source_type = sourceType,
              ingest_source_name = sourceName,
              ingest_ts = now
            )
          )
        }
      }
  }
}
