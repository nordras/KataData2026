package com.katadata.api.handler

import com.sun.net.httpserver.{HttpExchange, HttpHandler}

import java.io.OutputStream
import java.nio.charset.StandardCharsets

abstract class BaseHttpHandler extends HttpHandler {

  protected def sendJson(exchange: HttpExchange, statusCode: Int, body: String): Unit = {
    try {
      val bytes = body.getBytes(StandardCharsets.UTF_8)
      exchange.getResponseHeaders.add("Content-Type", "application/json; charset=utf-8")

      exchange.sendResponseHeaders(statusCode, bytes.length)

      val output: OutputStream = exchange.getResponseBody
      output.write(bytes)
      output.close()
    } catch {
      case ex: Exception =>
        println(s"[ERROR] Erro ao enviar resposta: ${ex.getMessage}")
    }
  }

  protected def isGetRequest(exchange: HttpExchange): Boolean = {
    exchange.getRequestMethod == "GET"
  }

  protected def methodNotAllowed(exchange: HttpExchange): Unit = {
    sendJson(exchange, 405, """{"error":"method not allowed"}""")
  }

  protected def serverError(exchange: HttpExchange, message: String): Unit = {
    val escapedMsg = message.replace("\"", "\\\"")
    sendJson(exchange, 500, s"""{"error":"$escapedMsg"}""")
  }
}
