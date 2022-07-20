package com.qwshen.etl.common.stream

import org.apache.spark.sql.{ForeachWriter, Row}
import com.qwshen.common.{PropertyInitializer, PropertyValidator}

/**
 * Continuous Writer for stream sink
 */
trait ContinuousWriter extends ForeachWriter[Row] with Serializable {
  def write(rows: Seq[Row], batchId: Option[Long]): Unit
}
