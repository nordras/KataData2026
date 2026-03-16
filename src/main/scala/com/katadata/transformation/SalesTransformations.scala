package com.katadata.transformation

import org.apache.flink.table.api.{Table, TableEnvironment}

object SalesTransformations {

  def normalizeCurated(tableEnv: TableEnvironment): Table = {
    tableEnv.sqlQuery(
      """
        |SELECT
        |  sale_id,
        |  sale_ts,
        |  city,
        |  salesman_id,
        |  salesman_name,
        |  CAST(amount AS DOUBLE) AS amount,
        |  source_system,
        |  ingest_run_id,
        |  ingest_source_type,
        |  ingest_source_name,
        |  ingest_ts,
        |  CURRENT_TIMESTAMP AS curated_ts
        |FROM landing_sales
        |""".stripMargin
    )
  }

  def calculateTopSalesPerCity(tableEnv: TableEnvironment): Table = {
    tableEnv.sqlQuery(
      """
        |SELECT city, salesman_name, total_sales
        |FROM (
        |  SELECT
        |    city,
        |    salesman_name,
        |    SUM(amount) AS total_sales,
        |    RANK() OVER (PARTITION BY city ORDER BY SUM(amount) DESC) AS rnk
        |  FROM curated_sales
        |  GROUP BY city, salesman_name
        |) t
        |WHERE rnk = 1
        |""".stripMargin
    )
  }

  def calculateTopSalesmanCountry(tableEnv: TableEnvironment): Table = {
    tableEnv.sqlQuery(
      """
        |SELECT salesman_name, total_sales
        |FROM (
        |  SELECT
        |    salesman_name,
        |    SUM(amount) AS total_sales,
        |    RANK() OVER (ORDER BY SUM(amount) DESC) AS rnk
        |  FROM curated_sales
        |  GROUP BY salesman_name
        |) t
        |WHERE rnk = 1
        |""".stripMargin
    )
  }
}
