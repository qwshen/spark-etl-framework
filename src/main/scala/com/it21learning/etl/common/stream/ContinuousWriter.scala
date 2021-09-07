package com.it21learning.etl.common.stream

import org.apache.spark.sql.{ForeachWriter, Row}
import com.it21learning.common.{PropertyInitializer, PropertyValidater}

/**
 * Continuous Writer for stream sink
 */
trait ContinuousWriter extends ForeachWriter[Row] with PropertyInitializer with PropertyValidater with Serializable {
  def write(rows: Seq[Row], batchId: Option[Long]): Unit
}
