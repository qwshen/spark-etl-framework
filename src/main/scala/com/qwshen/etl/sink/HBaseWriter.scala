package com.qwshen.etl.sink

import com.qwshen.common.PropertyKey
import com.qwshen.common.io.HBaseChannel
import com.qwshen.etl.common.HBaseWriteActor
import com.qwshen.etl.sink.process.HBaseMicroBatchWriter
import com.typesafe.config.Config
import org.apache.hadoop.hbase.client.Connection
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * To write data-frame to HBase
 */
final class HBaseWriter extends HBaseWriteActor[HBaseWriter] {
  @PropertyKey("mode", true)
  //the mode for writing to HBase
  private var _mode: Option[String] = None

  //truncate the hbase table if it is to over-write
  override protected def preWrite(connection: Connection): Unit = for {
    table <- this._table
    mode <- this._mode
  } {
    if (mode == "overwrite") {
      HBaseChannel.truncate(table, connection)
    }
  }

  //the write method
  protected def write(props: Map[String, String], keyColumns: Seq[String], securityToken: Option[String])(df: DataFrame): Unit = for {
    table <- this._table
  } {
    new HBaseMicroBatchWriter(props, table, this._options, keyColumns, this._keyConcatenator, this._fieldsMapping, securityToken).write(df, 0L)
  }

  /**
   * Initialize the kafka reader
   */
  override def init(properties: Seq[(String, String)], config: Config)(implicit session: SparkSession): Unit = {
    super.init(properties, config)

    validate(this._mode, "The mode in HBaseWriter is mandatory.", Seq("overwrite", "merge"))
  }

  /**
   * The write mode - must be one of overwrite, merge.
   *
   * @param mode
   * @return
   */
  def writeMode(mode: String): HBaseWriter = { this._mode = Some(mode); this }
}
