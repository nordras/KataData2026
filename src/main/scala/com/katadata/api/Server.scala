package com.katadata.api

import com.sun.net.httpserver.{HttpExchange, HttpHandler, HttpServer}

import java.io.OutputStream
import java.net.InetSocketAddress
import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Paths}
import java.sql.{Connection, DriverManager, ResultSet}

object Server {
  def main(args: Array[String]): Unit = {
    val dbPath = if (args.nonEmpty) args(0) else "data/analytics.db"
    val port = if (args.length > 1) args(1).toInt else 8080
    val jdbcUrl = s"jdbc:sqlite:$dbPath"

    Class.forName("org.sqlite.JDBC")

    val server = HttpServer.create(new InetSocketAddress(port), 0)
    server.createContext("/health", new HealthHandler(dbPath))
    server.createContext("/top-sales-per-city", new QueryHandler(jdbcUrl, cityQuery))
    server.createContext("/top-salesman-country", new QueryHandler(jdbcUrl, countryQuery))
    server.setExecutor(null)
    server.start()

    println(s"API started at http://localhost:$port")
    println("Endpoints: /health, /top-sales-per-city, /top-salesman-country")
  }

  private val cityQuery =
    "SELECT city, salesman_name, total_sales, rank FROM top_sales_per_city ORDER BY city ASC"

  private val countryQuery =
    "SELECT salesman_name, total_sales FROM top_salesman_country ORDER BY total_sales DESC"

  private class HealthHandler(dbPath: String) extends HttpHandler {
    override def handle(exchange: HttpExchange): Unit = {
      if (exchange.getRequestMethod != "GET") {
        sendJson(exchange, 405, "{\"error\":\"method not allowed\"}")
        return
      }

      val exists = Files.exists(Paths.get(dbPath))
      val body = s"""{"status":"ok","db_exists":$exists}"""
      sendJson(exchange, 200, body)
    }
  }

  private class QueryHandler(jdbcUrl: String, query: String) extends HttpHandler {
    override def handle(exchange: HttpExchange): Unit = {
      if (exchange.getRequestMethod != "GET") {
        sendJson(exchange, 405, "{\"error\":\"method not allowed\"}")
        return
      }

      var connection: Connection = null
      var statement: java.sql.Statement = null
      var resultSet: ResultSet = null

      try {
        connection = DriverManager.getConnection(jdbcUrl)
        statement = connection.createStatement()
        resultSet = statement.executeQuery(query)

        val json = resultSetToJson(resultSet)
        sendJson(exchange, 200, s"""{"data":$json}""")
      } catch {
        case ex: Exception =>
          val error = s"""{"error":"${escapeJson(ex.getMessage)}"}"""
          sendJson(exchange, 500, error)
      } finally {
        if (resultSet != null) resultSet.close()
        if (statement != null) statement.close()
        if (connection != null) connection.close()
      }
    }

    private def resultSetToJson(rs: ResultSet): String = {
      val meta = rs.getMetaData
      val count = meta.getColumnCount
      val items = new StringBuilder("[")
      var firstRow = true

      while (rs.next()) {
        if (!firstRow) items.append(",")
        firstRow = false

        items.append("{")
        var col = 1
        while (col <= count) {
          if (col > 1) items.append(",")
          val name = meta.getColumnName(col)
          val value = rs.getObject(col)
          items.append("\"").append(escapeJson(name)).append("\":")
          items.append(formatValue(value))
          col += 1
        }
        items.append("}")
      }

      items.append("]")
      items.toString()
    }

    private def formatValue(value: AnyRef): String = {
      if (value == null) "null"
      else {
        value match {
          case n: java.lang.Number => n.toString
          case b: java.lang.Boolean => b.toString
          case other => "\"" + escapeJson(other.toString) + "\""
        }
      }
    }
  }

  private def sendJson(exchange: HttpExchange, code: Int, body: String): Unit = {
    val bytes = body.getBytes(StandardCharsets.UTF_8)
    exchange.getResponseHeaders.add("Content-Type", "application/json; charset=utf-8")
    exchange.sendResponseHeaders(code, bytes.length)
    val output: OutputStream = exchange.getResponseBody
    output.write(bytes)
    output.close()
  }

  private def escapeJson(text: String): String = {
    if (text == null) ""
    else text.replace("\\", "\\\\").replace("\"", "\\\"").replace("\n", "\\n")
  }
}
