package com.katadata.persistence

import org.apache.spark.sql.{DataFrame, SparkSession}

import java.sql.Timestamp
import java.util.Properties

class SalesRepository(sqliteJdbcUrl: String) {

  private val jdbcProperties = createJdbcProperties()

  private def createJdbcProperties(): Properties = {
    val props = new Properties()
    props.setProperty("driver", "org.sqlite.JDBC")
    props
  }

  def writeLandingSales(spark: SparkSession, df: DataFrame): Unit = {
    df.write
      .mode("append")
      .jdbc(sqliteJdbcUrl, "landing_sales", jdbcProperties)
  }

  def writeCuratedSales(spark: SparkSession, df: DataFrame): Unit = {
    df.write
      .mode("append")
      .jdbc(sqliteJdbcUrl, "curated_sales", jdbcProperties)
  }

  def writeTopSalesPerCity(spark: SparkSession, df: DataFrame): Unit = {
    df.write
      .mode("overwrite")
      .jdbc(sqliteJdbcUrl, "top_sales_per_city", jdbcProperties)
  }

  def writeTopSalesmanCountry(spark: SparkSession, df: DataFrame): Unit = {
    df.write
      .mode("overwrite")
      .jdbc(sqliteJdbcUrl, "top_salesman_country", jdbcProperties)
  }

  def writePipelineRun(
      spark: SparkSession,
      runId: String,
      sourceType: String,
      status: String,
      startedAt: Timestamp,
      endedAt: Timestamp,
      landingCount: Long,
      curatedCount: Long,
      topCityCount: Long,
      topCountryCount: Long,
      errorMessage: String
  ): Unit = {
    import spark.implicits._

    Seq(
      (
        runId,
        sourceType,
        status,
        startedAt,
        endedAt,
        landingCount,
        curatedCount,
        topCityCount,
        topCountryCount,
        errorMessage
      )
    ).toDF(
      "run_id",
      "source_type",
      "status",
      "started_at",
      "ended_at",
      "landing_count",
      "curated_count",
      "top_sales_per_city_count",
      "top_salesman_country_count",
      "error_message"
    ).write
      .mode("append")
      .jdbc(sqliteJdbcUrl, "pipeline_runs", jdbcProperties)
  }
}
