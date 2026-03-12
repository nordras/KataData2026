package com.katadata.api.handler

import com.sun.net.httpserver.HttpExchange

import java.nio.file.Files
import java.nio.file.Paths

class HealthHandler(dbPath: String) extends BaseHttpHandler {

  override def handle(exchange: HttpExchange): Unit = {
    try {
      if (!isGetRequest(exchange)) {
        methodNotAllowed(exchange)
        return
      }

      val dbExists = Files.exists(Paths.get(dbPath))
      val body = s"""{"status":"ok","db_exists":$dbExists}"""

      sendJson(exchange, 200, body)
    } catch {
      case ex: Exception =>
        serverError(exchange, ex.getMessage)
    }
  }
}
