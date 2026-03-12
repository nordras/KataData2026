package com.katadata.api.util

import java.sql.ResultSet

object JsonFormatter {

  def resultSetToJson(rs: ResultSet): String = {
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
    if (value == null) {
      "null"
    } else {
      value match {
        case n: java.lang.Number => n.toString
        case b: java.lang.Boolean => b.toString
        case other => "\"" + escapeJson(other.toString) + "\""
      }
    }
  }

  def escapeJson(text: String): String = {
    if (text == null) {
      ""
    } else {
      text
        .replace("\\", "\\\\")
        .replace("\"", "\\\"")
        .replace("\n", "\\n")
        .replace("\r", "\\r")
        .replace("\t", "\\t")
    }
  }

  def errorResponse(message: String): String = {
    s"""{"error":"${escapeJson(message)}"}"""
  }

  def dataResponse(data: String): String = {
    s"""{"data":$data}"""
  }

  def successResponse(message: String): String = {
    s"""{"status":"ok","message":"${escapeJson(message)}"}"""
  }
}
