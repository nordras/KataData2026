package com.katadata.api

import com.sun.net.httpserver.{HttpExchange, HttpServer}

import java.net.InetSocketAddress
import java.nio.charset.StandardCharsets
import java.sql.DriverManager

object Server {

  def main(args: Array[String]): Unit = {
    val dbPath = args.lift(0).getOrElse("data/analytics.db")
    val port = args.lift(1).flatMap(v => scala.util.Try(v.toInt).toOption).getOrElse(8080)
    val jdbcUrl = s"jdbc:sqlite:$dbPath"

    val server = HttpServer.create(new InetSocketAddress(port), 0)

    server.createContext("/health", (exchange: HttpExchange) => {
      sendJson(exchange, 200, "{\"status\":\"ok\"}")
    })

    server.createContext("/top-sales-per-city", (exchange: HttpExchange) => {
      if (exchange.getRequestMethod != "GET") {
        sendJson(exchange, 405, "{\"error\":\"method not allowed\"}")
      } else {
        val rows = query(jdbcUrl, "SELECT city, salesman_name, total_sales FROM top_sales_per_city ORDER BY city")
        sendJson(exchange, 200, rows)
      }
    })

    server.createContext("/top-salesman-country", (exchange: HttpExchange) => {
      if (exchange.getRequestMethod != "GET") {
        sendJson(exchange, 405, "{\"error\":\"method not allowed\"}")
      } else {
        val rows = query(jdbcUrl, "SELECT salesman_name, total_sales FROM top_salesman_country")
        sendJson(exchange, 200, rows)
      }
    })

    server.createContext("/pipeline-runs", (exchange: HttpExchange) => {
      if (exchange.getRequestMethod != "GET") {
        sendJson(exchange, 405, "{\"error\":\"method not allowed\"}")
      } else {
        val rows = query(
          jdbcUrl,
          "SELECT run_id, source_type, status, start_ts, end_ts, landing_count, curated_count, top_city_count, top_country_count, error_message FROM pipeline_runs ORDER BY end_ts DESC"
        )
        sendJson(exchange, 200, rows)
      }
    })

    server.start()
    println(s"API listening on http://localhost:$port")
  }

  private def query(jdbcUrl: String, sql: String): String = {
    val conn = DriverManager.getConnection(jdbcUrl)
    try {
      val stmt = conn.createStatement()
      val rs = stmt.executeQuery(sql)
      val md = rs.getMetaData
      val cols = md.getColumnCount

      val items = new StringBuilder("[")
      var firstRow = true
      while (rs.next()) {
        if (!firstRow) items.append(',')
        firstRow = false
        items.append('{')
        (1 to cols).foreach { i =>
          if (i > 1) items.append(',')
          val k = md.getColumnName(i)
          val v = Option(rs.getObject(i)).map(_.toString).getOrElse("")
          items.append('"').append(escape(k)).append('"').append(':').append('"').append(escape(v)).append('"')
        }
        items.append('}')
      }
      items.append(']')

      rs.close()
      stmt.close()
      items.toString()
    } finally {
      conn.close()
    }
  }

  private def sendJson(exchange: HttpExchange, status: Int, body: String): Unit = {
    val bytes = body.getBytes(StandardCharsets.UTF_8)
    exchange.getResponseHeaders.add("Content-Type", "application/json; charset=utf-8")
    exchange.sendResponseHeaders(status, bytes.length)
    val out = exchange.getResponseBody
    out.write(bytes)
    out.close()
  }

  private def escape(v: String): String = {
    v.replace("\\", "\\\\").replace("\"", "\\\"")
  }
}
