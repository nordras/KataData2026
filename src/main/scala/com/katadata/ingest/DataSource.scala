package com.katadata.ingest

import com.katadata.config.PipelineConfig
import com.katadata.model.Sale

trait DataSource {
  def load(config: PipelineConfig): Seq[Sale]
}
