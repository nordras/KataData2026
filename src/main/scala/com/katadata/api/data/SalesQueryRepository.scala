package com.katadata.api.data

import com.katadata.api.util.JsonFormatter

import java.sql.{Connection, DriverManager, ResultSet, Statement}

class SalesQueryRepository(jdbcUrl: String) {

  def getTopSalesPerCity(): String = {
    val query = "SELECT city, salesman_name, total_sales, rank FROM top_sales_per_city ORDER BY city ASC"
    executeQuery(query)
  }

  def getTopSalesmanCountry(): String = {
    val query = "SELECT salesman_name, total_sales FROM top_salesman_country ORDER BY total_sales DESC"
    executeQuery(query)
  }

  def getPipelineRuns(): String = {
    val query =
      "SELECT run_id, source_type, status, started_at, ended_at, landing_count, curated_count, top_sales_per_city_count, top_salesman_country_count, error_message FROM pipeline_runs ORDER BY started_at DESC LIMIT 20"
    executeQuery(query)
  }

  private def executeQuery(query: String): String = {
    var connection: Connection = null
    var statement: Statement = null
    var resultSet: ResultSet = null

    try {
      connection = DriverManager.getConnection(jdbcUrl)
      statement = connection.createStatement()
      resultSet = statement.executeQuery(query)

      val json = JsonFormatter.resultSetToJson(resultSet)
      json
    } finally {
      if (resultSet != null) resultSet.close()
      if (statement != null) statement.close()
      if (connection != null) connection.close()
    }
  }
}
