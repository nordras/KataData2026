package com.katadata.ingest

import com.katadata.config.PipelineConfig
import com.katadata.model.Sale

import java.net.URI
import java.net.http.{HttpClient, HttpRequest, HttpResponse}
import java.time.Duration

class WebServiceSource extends DataSource {

  override def load(config: PipelineConfig): Seq[Sale] = {
    val bodyOpt = fetchBody(config.wsUrl)

    bodyOpt match {
      case Some(body) => parseCsvBody(body, config)
      case None => fallback(config)
    }
  }

  private def fetchBody(url: String): Option[String] = {
    val client = HttpClient.newBuilder()
      .connectTimeout(Duration.ofSeconds(3))
      .build()

    val req = HttpRequest.newBuilder()
      .uri(URI.create(url))
      .timeout(Duration.ofSeconds(5))
      .GET()
      .build()

    scala.util.Try(client.send(req, HttpResponse.BodyHandlers.ofString()))
      .toOption
      .filter(_.statusCode() >= 200)
      .filter(_.statusCode() < 300)
      .map(_.body())
  }

  private def parseCsvBody(body: String, config: PipelineConfig): Seq[Sale] = {
    val lines = body.split("\\r?\\n").toSeq.filter(_.trim.nonEmpty)
    val now = java.time.Instant.now().toString

    lines.drop(1).flatMap { line =>
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
            ingest_run_id = config.runId,
            ingest_source_type = "ws",
            ingest_source_name = config.wsUrl,
            ingest_ts = now
          )
        )
      }
    }
  }

  private def fallback(config: PipelineConfig): Seq[Sale] = {
    CsvSupport.readSalesFromCsv(
      path = "src/main/resources/sample_sales.csv",
      sourceType = "ws",
      sourceName = "ws-simulated-fallback",
      runId = config.runId
    )
  }
}
