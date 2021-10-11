package com.qwshen.etl.sink.process

import com.qwshen.etl.common.stream.MicroBatchWriter
import org.apache.spark.sql.DataFrame
import scala.util.{Failure, Success, Try}
import com.qwshen.etl.utils.DataframeSplitter._

/**
 * Write the rows into HBase.
 */
private[etl] final class HBaseMicroBatchWriter(connectionProperties: Map[String, String], table: String, options: Map[String, String], keyColumns: Seq[String], keyConcatenator: Option[String], fieldsMapping: Map[String, String], securityToken: Option[String]) extends MicroBatchWriter {
  /**
   * Write the batch data-frame for the implementation of BatchWriter
   *
   * @param batchDf
   * @param batchId
   */
  def write(batchDf: DataFrame, batchId: Long): Unit = {
    val numPartitions = Try(options("numPartitions").toInt) match {
      case Success(num) => num
      case Failure(t) => throw new RuntimeException("The numPartitions cannot be empty, it must be set as a position valid number.")
    }
    val batchSize = Try(options("batchSize").toInt) match {
      case Success(num) => num
      case Failure(t) => throw new RuntimeException("The batchSize cannot be empty, it must be set as a position valid number.")
    }

    val bcProperties = batchDf.sparkSession.sparkContext.broadcast(connectionProperties)
    val bcTable = batchDf.sparkSession.sparkContext.broadcast(table)
    val bcKeyConcatenator = batchDf.sparkSession.sparkContext.broadcast(keyConcatenator)
    val bcKeyColumns = batchDf.sparkSession.sparkContext.broadcast(keyColumns)
    val bcFieldsMapping = batchDf.sparkSession.sparkContext.broadcast(fieldsMapping)
    val bcSecurityToken = batchDf.sparkSession.sparkContext.broadcast(securityToken)
    //write the batch data-frame
    batchDf.split(numPartitions).foreach(df => df.rdd.foreachPartition(rows => {
      //create the writer
      val writer = new HBaseContinuousWriter(bcProperties.value, bcTable.value, bcKeyColumns.value, bcKeyConcatenator.value, bcFieldsMapping.value, bcSecurityToken.value)
      //open connection
      writer.open(0L, 0L)
      Try {
        rows.grouped(batchSize).foreach(g => writer.write(g, None))
      } match {
        case Success(_) => writer.close(null)
        case Failure(t) => writer.close(t)
      }
    }))
  }
}
