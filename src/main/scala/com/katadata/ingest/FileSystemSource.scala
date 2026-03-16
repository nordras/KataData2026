package com.katadata.ingest

import com.katadata.config.PipelineConfig
import com.katadata.model.Sale

class FileSystemSource extends DataSource {
  override def load(config: PipelineConfig): Seq[Sale] = {
    CsvSupport.readSalesFromCsv(
      path = "src/main/resources/sample_sales.csv",
      sourceType = "file",
      sourceName = "sample_sales.csv",
      runId = config.runId
    )
  }
}
