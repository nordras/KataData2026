package com.katadata.model

import java.sql.Timestamp

case class Sale(
    saleId: String,
    saleTs: String,
    city: String,
    salesmanId: String,
    salesmanName: String,
    amount: Double,
    sourceSystem: String,
    ingestRunId: String,
    ingestSourceType: String,
    ingestSourceName: String,
    ingestTs: Timestamp
)

case class TopSalesPerCity(
    city: String,
    salesmanName: String,
    totalSales: Double,
    rank: Long
)

case class TopSalesmanCountry(
    salesmanName: String,
    totalSales: Double
)

case class PipelineRun(
    runId: String,
    sourceType: String,
    status: String,
    startedAt: Timestamp,
    endedAt: Timestamp,
    landingCount: Long,
    curatedCount: Long,
    topSalesPerCityCount: Long,
    topSalesmanCountryCount: Long,
    errorMessage: String
)
