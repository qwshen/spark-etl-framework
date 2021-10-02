package com.it21learning.etl.common

import com.it21learning.common.PropertyKey
import com.it21learning.common.io.HBaseChannel
import org.apache.hadoop.hbase.security.token.TokenUtil
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.hadoop.hbase.client.Connection
import java.io.File
import scala.util.{Failure, Success, Try}

private[etl] abstract class HBaseWriteActor[T] extends HBaseActor[T] { self: T =>
  //the key concatenator
  @PropertyKey("rowKey.concatenator", false)
  protected var _keyConcatenator: Option[String] = None
  @PropertyKey("rowKey.from", false)
  protected var _keyColumns: Option[String] = None
  //the field mapping
  @PropertyKey("columnsMapping.*", true)
  protected var _fieldsMapping: Map[String, String] = Map.empty[String, String]

  //the options for loading the file
  @PropertyKey("options.*", false)
  protected var _options: Map[String, String] = Map.empty[String, String]

  //the view for writing out
  @PropertyKey("view", true)
  protected var _view: Option[String] = None

  /**
   * Run the hbase-writer
   *
   * @param ctx - the execution context
   * @param session - the spark-session
   * @return
   */
  def run(ctx: ExecutionContext)(implicit session: SparkSession): Option[DataFrame] = for {
    table <- this._table
    df <- this._view.flatMap(name => ctx.getView(name))
  } yield Try {
    if (!this._options.contains("numPartitions")) {
      this.option("numPartitions", ctx.appCtx.ioConnections.toString)
    }
    if (!this._options.contains("batchSize")) {
      this.option("batchSize", ctx.appCtx.ioBatchSize.toString)
    }

    var kerberosToken: Option[String] = None
    val connection = HBaseChannel.open(this._connectionProperties)
    try {
      //obtain the kerberos token
      val token = TokenUtil.obtainToken(connection)
      kerberosToken = Some(token.encodeToUrlString())

      //if overwrite, truncate the target table first
      preWrite(connection)
    }
    finally {
      HBaseChannel.close(connection)
    }

    val newProps = this._connectionProperties.map(x => {
      if (x._1 == "hbase-site.xml" || x._1 == "core-site.xml" || x._1 == "hdfs-site.xml") {
        session.sparkContext.addFile(new File(x._2).getName)
        (x._1, x._1)
      }
      else {
        (x._1, x._2)
      }
    })
    val (keyColumns: Seq[String], dfOutput: DataFrame) = this._keyColumns match {
      case Some(kc) => (kc.split(",").toSeq, df)
      case _ =>
        val uuid = udf { () => java.util.UUID.randomUUID().toString }
        (Seq("__row_key"), df.withColumn("__row_key", uuid()))
    }
    write(newProps, keyColumns, kerberosToken)(dfOutput)
  } match {
    case Success(_) => df
    case Failure(ex) => throw new RuntimeException(s"Cannot write to target - ${this._table}.", ex)
  }

  //any process before actual write
  protected def preWrite(connection: Connection): Unit = {}
  //the write method
  protected def write(props: Map[String, String], keyColumns: Seq[String], securityToken: Option[String])(df: DataFrame): Unit

  /**
   * Single key column
   * @param key
   * @return
   */
  def KeyColumn(key: String): T = { this._keyColumns = Some(key); this }

  /**
   * Multiple key columns
   * @param keys
   * @return
   */
  def keyColumns(keys: String*): T = { this._keyColumns = Some(keys.mkString(",")); this }

  /**
   * The key-concatenator for combining multiple key-columns
   *
   * @param value
   * @return
   */
  def keyConcatenator(value: String): T = { this._keyConcatenator = Some(value); this }

  /**
   * The data field mapping from data-frame to target HBase table.
   *
   * @param srcName
   * @param dstName
   * @param isKey
   * @return
   */
  def dataField(srcName: String, dstName: String): T = { this._fieldsMapping = this._fieldsMapping + (srcName -> dstName); this }

  /**
   * The load option
   *
   * @param name
   * @param value
   * @return
   */
  def option(name: String, value: String): T = { this._options = this._options + (name -> value); this }
  /**
   * The load options
   *
   * @param opts
   * @return
   */
  def options(opts: Map[String, String]): T = { this._options = this._options ++ opts; this }

  /**
   * The source view for writing into HBase
   *
   * @param view
   * @return
   */
  def sourceView(view: String): T = { this._view = Some(view); this }
}
