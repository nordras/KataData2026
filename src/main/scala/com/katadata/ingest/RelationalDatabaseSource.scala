package com.katadata.ingest

import com.katadata.config.PipelineConfig
import com.katadata.model.Sale

import java.nio.file.{Files, Paths}
import java.sql.{Connection, DriverManager, ResultSet}

class RelationalDatabaseSource extends DataSource {

  override def load(config: PipelineConfig): Seq[Sale] = {
    ensureMockDb(config)

    var conn: Connection = null
    val sales = scala.collection.mutable.ArrayBuffer.empty[Sale]

    try {
      conn = DriverManager.getConnection(config.sqliteJdbcUrl)
      val stmt = conn.createStatement()
      val rs = stmt.executeQuery(
        """
          |SELECT sale_id, sale_ts, city, salesman_id, salesman_name, amount, source_system
          |FROM mock_sales_source
          |ORDER BY sale_id
          |""".stripMargin
      )

      while (rs.next()) {
        sales += mapToSale(rs, config)
      }

      rs.close()
      stmt.close()
      sales.toSeq
    } finally {
      if (conn != null) conn.close()
    }
  }

  private def mapToSale(rs: ResultSet, config: PipelineConfig): Sale = {
    Sale(
      sale_id = rs.getString("sale_id"),
      sale_ts = rs.getString("sale_ts"),
      city = rs.getString("city"),
      salesman_id = rs.getString("salesman_id"),
      salesman_name = rs.getString("salesman_name"),
      amount = rs.getDouble("amount"),
      source_system = rs.getString("source_system"),
      ingest_run_id = config.runId,
      ingest_source_type = "db",
      ingest_source_name = "mock_sales_source",
      ingest_ts = java.time.Instant.now().toString
    )
  }

  private def ensureMockDb(config: PipelineConfig): Unit = {
    val dbPath = Paths.get(config.sqliteDbPath)
    if (dbPath.getParent != null) {
      Files.createDirectories(dbPath.getParent)
    }

    var conn: Connection = null
    try {
      conn = DriverManager.getConnection(config.sqliteJdbcUrl)
      val stmt = conn.createStatement()

      stmt.execute(
        """
          |CREATE TABLE IF NOT EXISTS mock_sales_source (
          |  sale_id TEXT,
          |  sale_ts TEXT,
          |  city TEXT,
          |  salesman_id TEXT,
          |  salesman_name TEXT,
          |  amount REAL,
          |  source_system TEXT
          |)
          |""".stripMargin
      )

      val countRs = stmt.executeQuery("SELECT COUNT(*) FROM mock_sales_source")
      val hasRows = countRs.next() && countRs.getInt(1) > 0
      countRs.close()

      if (!hasRows) {
        val seed = CsvSupport.readSalesFromCsv(
          path = "src/main/resources/sample_sales.csv",
          sourceType = "db",
          sourceName = "mock_sales_source_seed",
          runId = config.runId
        )

        val ps = conn.prepareStatement(
          """
            |INSERT INTO mock_sales_source
            |(sale_id, sale_ts, city, salesman_id, salesman_name, amount, source_system)
            |VALUES (?, ?, ?, ?, ?, ?, ?)
            |""".stripMargin
        )

        seed.foreach { sale =>
          ps.setString(1, sale.sale_id)
          ps.setString(2, sale.sale_ts)
          ps.setString(3, sale.city)
          ps.setString(4, sale.salesman_id)
          ps.setString(5, sale.salesman_name)
          ps.setDouble(6, sale.amount)
          ps.setString(7, sale.source_system)
          ps.addBatch()
        }

        ps.executeBatch()
        ps.close()
      }

      stmt.close()
    } finally {
      if (conn != null) conn.close()
    }
  }
}
