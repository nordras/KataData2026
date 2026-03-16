package com.katadata.model

case class Sale(
  sale_id: String,
  sale_ts: String,
  city: String,
  salesman_id: String,
  salesman_name: String,
  amount: Double,
  source_system: String,
  ingest_run_id: String,
  ingest_source_type: String,
  ingest_source_name: String,
  ingest_ts: String
)
