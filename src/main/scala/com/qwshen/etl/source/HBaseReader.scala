package com.qwshen.etl.source

import com.qwshen.common.PropertyKey
import com.qwshen.common.io.HBaseChannel
import com.qwshen.etl.common.{ExecutionContext, HBaseActor}
import org.apache.spark.sql.{DataFrame, SparkSession}
import scala.util.{Failure, Success, Try}

/**
 * To write data-frame to HBase
 */
final class HBaseReader extends HBaseActor[HBaseReader] {
  @PropertyKey("columnsMapping.*", false)
  protected var _fieldsMapping: Map[String, String] = Map.empty[String, String]

  //the options for loading from hbase
  @PropertyKey("options.*", false)
  protected var _options: Map[String, String] = Map.empty[String, String]
  //filter the rows
  @PropertyKey("filter.*", false)
  protected var _filter: Map[String, String] = Map.empty[String, String]

  /**
   * Run the hbase-reader
   *
   * @param ctx - the execution context
   * @param session - the spark-session
   * @return
   */
  def run(ctx: ExecutionContext)(implicit session: SparkSession): Option[DataFrame] = for {
    table <- this._table
  } yield Try {
    HBaseChannel.read(this._connectionProperties, table)(this._fieldsMapping, this._options, this._filter)
  } match {
    case Success(df) => df
    case Failure(ex) => throw new RuntimeException(s"Cannot read from source - ${this._table}.", ex)
  }

  /**
   * The data field mapping from data-frame to target HBase table.
   *
   * @param srcName
   * @param dstName
   * @return
   */
  def dataField(srcName: String, dstName: String): HBaseReader = { this._fieldsMapping = this._fieldsMapping + (dstName -> srcName); this }

  /**
   * The load option
   *
   * @param name
   * @param value
   * @return
   */
  def option(name: String, value: String): HBaseReader = { this._options = this._options + (name -> value); this }
  /**
   * The load options
   *
   * @param opts
   * @return
   */
  def options(opts: Map[String, String]): HBaseReader = { this._options = this._options ++ opts; this }

  /**
   * The load filter
   *
   * @param name
   * @param value
   * @return
   */
  def filter(name: String, value: String): HBaseReader = { this._filter = this._filter + (name -> value); this }
  /**
   * The load options
   *
   * @param opts
   * @return
   */
  def filter(opts: Map[String, String]): HBaseReader = { this._filter = this._filter ++ opts; this }
}
