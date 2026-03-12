package com.katadata.transformation

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.{Window, WindowSpec}
import org.apache.spark.sql.functions.{col, current_timestamp, desc, rank, sum}

object SalesTransformations {

  def normalizeCurated(df: DataFrame): DataFrame = {
    df.select(
      col("sale_id"),
      col("sale_ts"),
      col("city"),
      col("salesman_id"),
      col("salesman_name"),
      col("amount").cast("double"),
      col("source_system"),
      col("ingest_run_id"),
      col("ingest_source_type"),
      col("ingest_source_name"),
      col("ingest_ts")
    ).withColumn("curated_ts", current_timestamp())
  }

  def calculateTopSalesPerCity(df: DataFrame): DataFrame = {
    val cityWindow: WindowSpec = Window
      .partitionBy(col("city"))
      .orderBy(desc("total_sales"))

    df.groupBy(col("city"), col("salesman_name"))
      .agg(sum(col("amount")).alias("total_sales"))
      .withColumn("rank", rank().over(cityWindow))
      .filter(col("rank") === 1)
  }

  def calculateTopSalesmanCountry(df: DataFrame): DataFrame = {
    df.groupBy(col("salesman_name"))
      .agg(sum(col("amount")).alias("total_sales"))
      .orderBy(desc("total_sales"))
      .limit(1)
  }
}
