package com.katadata.persistence

import com.katadata.model.Sale
import org.apache.flink.table.api.Table
import org.apache.flink.types.Row

import java.nio.file.{Files, Paths}
import java.sql.{Connection, DriverManager, PreparedStatement, Timestamp}
import java.time.Instant

class SalesRepository(jdbcUrl: String) {

  initTables()

  def writeLandingSales(rows: Seq[Sale]): Unit = {
    insertBatch(
      """
        |INSERT INTO landing_sales
        |(sale_id, sale_ts, city, salesman_id, salesman_name, amount, source_system,
        | ingest_run_id, ingest_source_type, ingest_source_name, ingest_ts)
        |VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        |""".stripMargin,
      rows
    ) { sale =>
      Seq(
        sale.sale_id,
        sale.sale_ts,
        sale.city,
        sale.salesman_id,
        sale.salesman_name,
        sale.amount,
        sale.source_system,
        sale.ingest_run_id,
        sale.ingest_source_type,
        sale.ingest_source_name,
        sale.ingest_ts
      )
      }
  }


  def overwriteCuratedSales(table: Table): Int = overwriteWithRows(
    table,
    "DELETE FROM curated_sales",
    """
      |INSERT INTO curated_sales
      |(sale_id, sale_ts, city, salesman_id, salesman_name, amount, source_system,
      | ingest_run_id, ingest_source_type, ingest_source_name, ingest_ts, curated_ts)
      |VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
      |""".stripMargin,
    row => {
      Seq(
        row.getField(0), row.getField(1), row.getField(2), row.getField(3), row.getField(4), row.getField(5),
        row.getField(6), row.getField(7), row.getField(8), row.getField(9), row.getField(10),
        Option(row.getField(11)).map(_.toString).orNull
      )
    }
  )

  def overwriteTopSalesPerCity(table: Table): Int = overwriteWithRows(
    table,
    "DELETE FROM top_sales_per_city",
    "INSERT INTO top_sales_per_city (city, salesman_name, total_sales) VALUES (?, ?, ?)",
    row => Seq(row.getField(0), row.getField(1), row.getField(2))
  )

  def overwriteTopSalesmanCountry(table: Table): Int = overwriteWithRows(
    table,
    "DELETE FROM top_salesman_country",
    "INSERT INTO top_salesman_country (salesman_name, total_sales) VALUES (?, ?)",
    row => Seq(row.getField(0), row.getField(1))
  )
  def writePipelineRun(
    runId: String,
    sourceType: String,
    status: String,
    startTs: Instant,
    endTs: Instant,
    landingCount: Long,
    curatedCount: Long,
    topCityCount: Long,
    topCountryCount: Long,
    errorMessage: String
  ): Unit = {
    withConnection { conn =>
      val ps = conn.prepareStatement(
        """
          |INSERT INTO pipeline_runs
          |(run_id, source_type, status, start_ts, end_ts, landing_count, curated_count,
          | top_city_count, top_country_count, error_message)
          |VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
          |""".stripMargin
      )
      ps.setString(1, runId)
      ps.setString(2, sourceType)
      ps.setString(3, status)
      ps.setTimestamp(4, Timestamp.from(startTs))
      ps.setTimestamp(5, Timestamp.from(endTs))
      ps.setLong(6, landingCount)
      ps.setLong(7, curatedCount)
      ps.setLong(8, topCityCount)
      ps.setLong(9, topCountryCount)
      ps.setString(10, errorMessage)
      ps.executeUpdate()
      ps.close()
    }
  }

  private def overwriteWithRows(
    table: Table,
    deleteSql: String,
    insertSql: String,
    rowMapper: Row => Seq[Any]
  ): Int = {
    val rows = table.execute().collect()
    withConnection { conn =>
      conn.createStatement().execute(deleteSql)
      val ps = conn.prepareStatement(insertSql)
      var count = 0

      while (rows.hasNext) {
        val values = rowMapper(rows.next())
        bind(ps, values)
        ps.addBatch()
        count += 1
      }

      ps.executeBatch()
      ps.close()
      count
    }
  }

  private def bind(ps: PreparedStatement, values: Seq[Any]): Unit = {
    values.zipWithIndex.foreach {
      case (v, idx) =>
        v match {
          case null => ps.setObject(idx + 1, null)
          case d: Double => ps.setDouble(idx + 1, d)
          case f: Float => ps.setDouble(idx + 1, f.toDouble)
          case l: Long => ps.setLong(idx + 1, l)
          case i: Int => ps.setInt(idx + 1, i)
          case other => ps.setString(idx + 1, other.toString)
        }
    }
  }

  private def insertBatch[T](sql: String, rows: Seq[T])(toValues: T => Seq[Any]): Unit = {
    if (rows.isEmpty) {
      return
    }

    withConnection { conn =>
      val ps = conn.prepareStatement(sql)
      try {
        rows.foreach { row =>
          bind(ps, toValues(row))
          ps.addBatch()
        }
        ps.executeBatch()
      } finally {
        ps.close()
      }
    }
  }

  private def withConnection[A](f: Connection => A): A = {
    var conn: Connection = null
    try {
      conn = DriverManager.getConnection(jdbcUrl)
      f(conn)
    } finally {
      if (conn != null) conn.close()
    }
  }

  private def initTables(): Unit = {
    ensureDirectory()
    withConnection { conn =>
      val stmt = conn.createStatement()

      stmt.execute(
        """
          |CREATE TABLE IF NOT EXISTS landing_sales (
          |  sale_id TEXT,
          |  sale_ts TEXT,
          |  city TEXT,
          |  salesman_id TEXT,
          |  salesman_name TEXT,
          |  amount REAL,
          |  source_system TEXT,
          |  ingest_run_id TEXT,
          |  ingest_source_type TEXT,
          |  ingest_source_name TEXT,
          |  ingest_ts TEXT
          |)
          |""".stripMargin
      )

      stmt.execute(
        """
          |CREATE TABLE IF NOT EXISTS curated_sales (
          |  sale_id TEXT,
          |  sale_ts TEXT,
          |  city TEXT,
          |  salesman_id TEXT,
          |  salesman_name TEXT,
          |  amount REAL,
          |  source_system TEXT,
          |  ingest_run_id TEXT,
          |  ingest_source_type TEXT,
          |  ingest_source_name TEXT,
          |  ingest_ts TEXT,
          |  curated_ts TEXT
          |)
          |""".stripMargin
      )

      stmt.execute(
        """
          |CREATE TABLE IF NOT EXISTS top_sales_per_city (
          |  city TEXT,
          |  salesman_name TEXT,
          |  total_sales REAL
          |)
          |""".stripMargin
      )

      stmt.execute(
        """
          |CREATE TABLE IF NOT EXISTS top_salesman_country (
          |  salesman_name TEXT,
          |  total_sales REAL
          |)
          |""".stripMargin
      )

      stmt.execute(
        """
          |CREATE TABLE IF NOT EXISTS pipeline_runs (
          |  run_id TEXT,
          |  source_type TEXT,
          |  status TEXT,
          |  start_ts TEXT,
          |  end_ts TEXT,
          |  landing_count INTEGER,
          |  curated_count INTEGER,
          |  top_city_count INTEGER,
          |  top_country_count INTEGER,
          |  error_message TEXT
          |)
          |""".stripMargin
      )

      stmt.close()
    }
  }

  private def ensureDirectory(): Unit = {
    val path = jdbcUrl.stripPrefix("jdbc:sqlite:")
    val p = Paths.get(path)
    if (p.getParent != null) {
      Files.createDirectories(p.getParent)
    }
  }
}
