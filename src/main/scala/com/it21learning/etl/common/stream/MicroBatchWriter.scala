package com.it21learning.etl.common.stream

import org.apache.spark.sql.DataFrame

/**
 * BatchWriter for stream sink
 */
trait MicroBatchWriter extends Serializable {
  /**
   * Write the batch data-frame
   *
   * @param batchDf
   * @param batchId
   */
  def write(batchDf: DataFrame, batchId: Long): Unit
}
