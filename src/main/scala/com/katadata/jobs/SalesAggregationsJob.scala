package com.katadata.jobs

import com.katadata.config.PipelineConfig
import com.katadata.ingest.DataSourceFactory
import com.katadata.persistence.SalesRepository
import com.katadata.transformation.SalesTransformations
import org.apache.flink.api.common.RuntimeExecutionMode
import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api.bridge.scala.StreamTableEnvironment

import java.time.Instant

object SalesAggregationsJob {

  def main(args: Array[String]): Unit = {
    val config = PipelineConfig.fromArgs(args)
    val startTs = Instant.now()

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setRuntimeMode(RuntimeExecutionMode.BATCH)
    env.setParallelism(config.parallelism)
    val tableEnv = StreamTableEnvironment.create(env)

    val repository = new SalesRepository(config.sqliteJdbcUrl)

    var landingCount = 0L
    var curatedCount = 0L
    var topCityCount = 0L
    var topCountryCount = 0L
    var status = "success"
    var errorMessage = ""

    try {
      println(s"[1/5] Ingestao simulada (${config.sourceType})")
      val source = DataSourceFactory.fromSourceType(config.sourceType)
      val landingRows = source.load(config)
      landingCount = landingRows.size
      repository.writeLandingSales(landingRows)

      val landingStream = env.fromCollection(landingRows)
      tableEnv.createTemporaryView("landing_sales", landingStream)

      println("[2/5] Curated")
      val curated = SalesTransformations.normalizeCurated(tableEnv)
      tableEnv.createTemporaryView("curated_sales", curated)
      curatedCount = repository.overwriteCuratedSales(curated)

      println("[3/5] Top sales per city")
      val topPerCity = SalesTransformations.calculateTopSalesPerCity(tableEnv)
      topCityCount = repository.overwriteTopSalesPerCity(topPerCity)

      println("[4/5] Top salesman country")
      val topCountry = SalesTransformations.calculateTopSalesmanCountry(tableEnv)
      topCountryCount = repository.overwriteTopSalesmanCountry(topCountry)

    } catch {
      case ex: Exception =>
        status = "failed"
        errorMessage = Option(ex.getMessage).getOrElse("unknown error")
        ex.printStackTrace()
        throw ex
    } finally {
      val endTs = Instant.now()
      repository.writePipelineRun(
        runId = config.runId,
        sourceType = config.sourceType,
        status = status,
        startTs = startTs,
        endTs = endTs,
        landingCount = landingCount,
        curatedCount = curatedCount,
        topCityCount = topCityCount,
        topCountryCount = topCountryCount,
        errorMessage = errorMessage
      )
      println(s"[5/5] Pipeline run saved: ${config.runId}")
    }
  }
}
