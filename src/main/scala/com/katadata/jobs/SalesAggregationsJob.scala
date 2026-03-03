package com.katadata.jobs

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, desc, rank, sum}
import org.apache.spark.sql.expressions.Window

object SalesAggregationsJob {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("KataData - Sales Aggregations")
      .master("local[*]")
      .getOrCreate()

    val inputPath = if (args.nonEmpty) args(0) else "src/main/resources/sample_sales.csv"

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

    println("=== Top Sales per City ===")
    rankedTopSalesPerCity.show(false)

    println("=== Top Salesman in Country ===")
    topSalesmanCountry.show(false)

    spark.stop()
  }
}
