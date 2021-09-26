package com.it21learning.etl.sink.process

import com.it21learning.etl.common.stream.MicroBatchWriter
import org.apache.spark.sql.DataFrame
import scala.util.{Failure, Success, Try}

/**
 * To write batch rows into target database. This is normally used in spark micro-batch/continuous streaming.
 */
private[etl] final class JdbcMicroBatchWriter(options: Map[String, String], sinkStmt: Option[String]) extends MicroBatchWriter {
    /**
   * Write the batch data-frame for the implementation of BatchWriter
   *
   * @param batchDf
   * @param batchId
   */
  def write(batchDf: DataFrame, batchId: Long): Unit = for {
    numPartitions <- Try(this.options("numPartitions").toInt).toOption
    batchSize <- Try(this.options("batchSize").toInt).toOption
  } {
    val bcDbOptions = batchDf.sparkSession.sparkContext.broadcast(this.options)
    val bcSinkStmt = batchDf.sparkSession.sparkContext.broadcast(this.sinkStmt)
    val bcBatchId = batchDf.sparkSession.sparkContext.broadcast(batchId)
    //write the batch data-frame
    batchDf.coalesce(numPartitions).rdd.foreachPartition(rows => {
      //create the writer
      val writer = new JdbcContinuousWriter(bcDbOptions.value, bcSinkStmt.value)
      //open connection
      writer.open(0L, 0L)
      Try {
        rows.grouped(batchSize).foreach(g => writer.write(g, Some(bcBatchId.value)))
      } match {
        case Success(_) => writer.close(null)
        case Failure(t) => writer.close(t)
      }
    })
  }
}