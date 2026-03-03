package com.katadata.jobs

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, desc, rank, sum}
import org.apache.spark.sql.expressions.Window

import java.nio.file.{Files, Paths}
import java.util.Properties

object SalesAggregationsJob {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("KataData - Sales Aggregations")
      .master("local[*]")
      .getOrCreate()

    val inputPath = if (args.nonEmpty) args(0) else "src/main/resources/sample_sales.csv"
    val sqliteDbPath = if (args.length > 1) args(1) else "data/analytics.db"
    val sqliteJdbcUrl = s"jdbc:sqlite:$sqliteDbPath"
    val sqlitePath = Paths.get(sqliteDbPath)
    val sqliteParent = sqlitePath.getParent
    if (sqliteParent != null) {
      Files.createDirectories(sqliteParent)
    }

    val salesDf = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv(inputPath)

    val topSalesPerCity = salesDf
      .groupBy(col("city"), col("salesman_name"))
      .agg(sum(col("amount")).alias("total_sales"))

    val cityWindow = Window.partitionBy(col("city")).orderBy(desc("total_sales"))
    val rankedTopSalesPerCity = topSalesPerCity
      .withColumn("rank", rank().over(cityWindow))
      .filter(col("rank") === 1)

    val topSalesmanCountry = salesDf
      .groupBy(col("salesman_name"))
      .agg(sum(col("amount")).alias("total_sales"))
      .orderBy(desc("total_sales"))
      .limit(1)

    val jdbcProperties = new Properties()
    jdbcProperties.setProperty("driver", "org.sqlite.JDBC")

    rankedTopSalesPerCity
      .write
      .mode("overwrite")
      .jdbc(sqliteJdbcUrl, "top_sales_per_city", jdbcProperties)

    topSalesmanCountry
      .write
      .mode("overwrite")
      .jdbc(sqliteJdbcUrl, "top_salesman_country", jdbcProperties)

    println("=== Top Sales per City ===")
    rankedTopSalesPerCity.show(false)

    println("=== Top Salesman in Country ===")
    topSalesmanCountry.show(false)

    println(s"=== SQLite Integration ===")
    println(s"DB Path: $sqliteDbPath")
    println("Tables written: top_sales_per_city, top_salesman_country")

    spark.stop()
  }
}
