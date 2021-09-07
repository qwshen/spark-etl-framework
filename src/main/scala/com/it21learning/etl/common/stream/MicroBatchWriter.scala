package com.it21learning.etl.common.stream

import org.apache.spark.sql.DataFrame
import com.it21learning.common.{PropertyInitializer, PropertyValidater}

/**
 * BatchWriter for stream sink
 */
trait MicroBatchWriter extends PropertyInitializer with PropertyValidater with Serializable {
  /**
   * Write the batch data-frame
   *
   * @param batchDf
   * @param batchId
   */
  def write(batchDf: DataFrame, batchId: Long): Unit
}
