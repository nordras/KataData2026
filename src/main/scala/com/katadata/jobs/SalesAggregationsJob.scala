package com.katadata.jobs

import com.katadata.config.PipelineConfig
import com.katadata.ingest.DataSourceFactory
import com.katadata.persistence.SalesRepository
import com.katadata.transformation.SalesTransformations
import org.apache.spark.sql.SparkSession

import java.nio.file.{Files, Paths}
import java.sql.Timestamp
import java.time.Instant

object SalesAggregationsJob {

  def main(args: Array[String]): Unit = {
    val config = PipelineConfig.fromArgs(args)
    val startTs = Instant.now()

    ensureDbDirectory(config.sqliteDbPath)

    val spark = createSparkSession()

    var landingCount = 0L
    var curatedCount = 0L
    var topCityCount = 0L
    var topCountryCount = 0L
    var status = "success"
    var errorMessage = ""

    try {
      val repository = new SalesRepository(config.sqliteJdbcUrl)

      println(s"[1/5] Ingestão de dados (source: ${config.sourceType})")
      val dataSource = DataSourceFactory.fromSourceType(config.sourceType)
      val landingDf = dataSource.read(spark, config.runId)
      landingCount = landingDf.count()
      repository.writeLandingSales(spark, landingDf)
      println(s"      ✓ Landing count: $landingCount")

      println("[2/5] Normalização e limpeza de dados")
      val curatedDf = SalesTransformations.normalizeCurated(landingDf)
      curatedCount = curatedDf.count()
      repository.writeCuratedSales(spark, curatedDf)
      println(s"      ✓ Curated count: $curatedCount")

      println("[3/5] Agregação: Top sales per city")
      val topSalesPerCity = SalesTransformations.calculateTopSalesPerCity(curatedDf)
      topCityCount = topSalesPerCity.count()
      repository.writeTopSalesPerCity(spark, topSalesPerCity)
      println(s"      ✓ Top sales per city count: $topCityCount")
      println("=== Top Sales per City ===")
      topSalesPerCity.show(false)

      println("[4/5] Agregação: Top salesman country")
      val topSalesmanCountry = SalesTransformations.calculateTopSalesmanCountry(curatedDf)
      topCountryCount = topSalesmanCountry.count()
      repository.writeTopSalesmanCountry(spark, topSalesmanCountry)
      println(s"      ✓ Top salesman country count: $topCountryCount")
      println("=== Top Salesman in Country ===")
      topSalesmanCountry.show(false)

    } catch {
      case ex: Exception =>
        status = "failed"
        errorMessage = Option(ex.getMessage).getOrElse("unknown error")
        println(s"[ERROR] ${ex.getMessage}")
        ex.printStackTrace()
        throw ex
    } finally {
      println("[5/5] Registrando metadata da execução")
      val endTs = Instant.now()

      try {
        val repository = new SalesRepository(config.sqliteJdbcUrl)
        repository.writePipelineRun(
          spark,
          config.runId,
          config.sourceType,
          status,
          Timestamp.from(startTs),
          Timestamp.from(endTs),
          landingCount,
          curatedCount,
          topCityCount,
          topCountryCount,
          errorMessage
        )
      } catch {
        case ex: Exception =>
          println(s"[WARNING] Falha ao registrar metadata: ${ex.getMessage}")
      }

      printExecutionSummary(
        config.runId,
        config.sourceType,
        status,
        startTs,
        endTs,
        landingCount,
        curatedCount,
        topCityCount,
        topCountryCount,
        config.sqliteDbPath
      )

      spark.stop()
    }
  }

  private def ensureDbDirectory(dbPath: String): Unit = {
    val parent = Paths.get(dbPath).getParent
    if (parent != null) {
      Files.createDirectories(parent)
    }
  }

  private def createSparkSession(): SparkSession = {
    SparkSession
      .builder()
      .appName("KataData - Sales Aggregations")
      .master("local[*]")
      .getOrCreate()
  }

  private def printExecutionSummary(
      runId: String,
      sourceType: String,
      status: String,
      startTs: Instant,
      endTs: Instant,
      landingCount: Long,
      curatedCount: Long,
      topCityCount: Long,
      topCountryCount: Long,
      dbPath: String
  ): Unit = {
    val duration = java.time.Duration.between(startTs, endTs).getSeconds
    println("\n" + "=" * 60)
    println("=== Run Metadata ===")
    println(s"Run ID:                    $runId")
    println(s"Source Type:               $sourceType")
    println(s"Status:                    $status")
    println(s"Duration:                  ${duration}s")
    println(s"Landing Count:             $landingCount")
    println(s"Curated Count:             $curatedCount")
    println(s"Top Sales Per City:        $topCityCount")
    println(s"Top Salesman Country:      $topCountryCount")
    println(s"Database Path:             $dbPath")
    println("Warehouse Layers:          landing_sales, curated_sales, top_sales_per_city, top_salesman_country")
    println("=" * 60 + "\n")
  }
}
