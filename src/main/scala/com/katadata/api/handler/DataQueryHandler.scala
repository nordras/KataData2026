package com.katadata.api.handler

import com.katadata.api.data.SalesQueryRepository
import com.katadata.api.util.JsonFormatter
import com.sun.net.httpserver.HttpExchange

class DataQueryHandler(repository: SalesQueryRepository, queryFn: SalesQueryRepository => String, endpointName: String)
    extends BaseHttpHandler {

  override def handle(exchange: HttpExchange): Unit = {
    try {
      if (!isGetRequest(exchange)) {
        methodNotAllowed(exchange)
        return
      }

      try {
        val jsonData = queryFn(repository)
        val response = JsonFormatter.dataResponse(jsonData)
        sendJson(exchange, 200, response)
      } catch {
        case ex: Exception =>
          println(s"[ERROR] Erro ao executar query em $endpointName: ${ex.getMessage}")
          serverError(exchange, s"Erro ao buscar dados: ${ex.getMessage}")
      }
    } catch {
      case ex: Exception =>
        println(s"[ERROR] Erro inesperado em $endpointName: ${ex.getMessage}")
        serverError(exchange, "Erro interno do servidor")
    }
  }
}
