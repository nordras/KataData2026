package com.katadata.api

import com.katadata.api.data.SalesQueryRepository
import com.katadata.api.handler.{DataQueryHandler, HealthHandler}
import com.sun.net.httpserver.HttpServer

import java.net.InetSocketAddress

object Server {

  def main(args: Array[String]): Unit = {
    val dbPath = if (args.nonEmpty) args(0) else "data/analytics.db"
    val port = if (args.length > 1) args(1).toInt else 8080
    val jdbcUrl = s"jdbc:sqlite:$dbPath"

    Class.forName("org.sqlite.JDBC")

    val repository = new SalesQueryRepository(jdbcUrl)

    val server = HttpServer.create(new InetSocketAddress(port), 0)

    server.createContext("/health", new HealthHandler(dbPath))
    server.createContext(
      "/top-sales-per-city",
      new DataQueryHandler(repository, _.getTopSalesPerCity(), "top-sales-per-city")
    )
    server.createContext(
      "/top-salesman-country",
      new DataQueryHandler(repository, _.getTopSalesmanCountry(), "top-salesman-country")
    )
    server.createContext("/pipeline-runs", new DataQueryHandler(repository, _.getPipelineRuns(), "pipeline-runs"))

    server.setExecutor(null)
    server.start()

    println(s"API http://localhost:$port")
    println("Endpoints disponíveis:")
    println("  GET /health                 - Health check")
    println("  GET /top-sales-per-city     - Top vendedor por cidade")
    println("  GET /top-salesman-country   - Top vendedor no país")
    println("  GET /pipeline-runs          - Histórico de execuções")
  }
}
