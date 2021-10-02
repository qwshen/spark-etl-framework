package com.it21learning.etl.sink

import com.it21learning.common.PropertyKey
import com.it21learning.etl.common.HBaseWriteActor
import com.it21learning.etl.sink.process.{HBaseContinuousWriter, HBaseMicroBatchWriter}
import org.apache.spark.sql.DataFrame

/**
 * To write data-frame to HBase
 */
final class HBaseStreamWriter extends HBaseWriteActor[HBaseStreamWriter] {
  //trigger mode
  @PropertyKey("trigger.mode", false)
  private var _triggerMode: Option[String] = None
  //trigger interval
  @PropertyKey("trigger.interval", false)
  private var _triggerInterval: Option[String] = None

  //the output mode
  @PropertyKey("outputMode", false)
  private var _outputMode: Option[String] = None
  //wait time in ms for test
  @PropertyKey("test.waittimeMS", false)
  private var _waittimeInMs: Option[Long] = None

  //the write method
  protected def write(props: Map[String, String], keyColumns: Seq[String], securityToken: Option[String])(df: DataFrame): Unit = for {
    table <- this._table
    mode <- this._outputMode
  } {
    //the initial writer
    val writer = this._options.foldLeft(df.writeStream)((q, o) => q.option(o._1, o._2)).outputMode(mode)

    //query for the streaming write
    val query = this._triggerMode match {
      case Some(m) if (m == "continuous") =>
        val bcProps = df.sparkSession.sparkContext.broadcast(props)
        val bcTable = df.sparkSession.sparkContext.broadcast(table)
        val bcKeyConcatenator = df.sparkSession.sparkContext.broadcast(this._keyConcatenator)
        val bcKeyColumns = df.sparkSession.sparkContext.broadcast(keyColumns)
        val bcFieldsMapping = df.sparkSession.sparkContext.broadcast(this._fieldsMapping)
        val bcSecurityToken = df.sparkSession.sparkContext.broadcast(securityToken)
        writer.foreach(new HBaseContinuousWriter(bcProps.value, bcTable.value, bcKeyColumns.value, bcKeyConcatenator.value, bcFieldsMapping.value, bcSecurityToken.value))
      case _ =>
        val batchWriter = new HBaseMicroBatchWriter(props, table, this._options, keyColumns, this._keyConcatenator, this._fieldsMapping, securityToken)
        writer.foreachBatch { (batchDf: DataFrame, batchId: Long) => batchWriter.write(batchDf, batchId) }
    }
    this._waittimeInMs match {
      case Some(ts) => query.start.awaitTermination(ts)
      case _ => query.start.awaitTermination()
    }
  }

  /**
   * The trigger mode
   *
   * @param mode
   * @return
   */
  def triggerMode(mode: String): HBaseStreamWriter = { this._triggerMode = Some(mode); this }

  /**
   * The trigger interval
   * @param duration
   * @return
   */
  def triggerInterval(duration: String): HBaseStreamWriter = { this._triggerInterval = Some(duration); this }

  /**
   * The output mode
   *
   * @param mode
   * @return
   */
  def outputMode(mode: String): HBaseStreamWriter = { this._outputMode = Some(mode); this }

  /**
   * Wait time for streaming to execute before shut it down
   *
   * @param waittime
   * @return
   */
  def waitTimeInMs(waittime: Long): HBaseStreamWriter = { this._waittimeInMs = Some(waittime); this }
}
