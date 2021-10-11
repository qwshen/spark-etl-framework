package com.qwshen.etl.common

import com.qwshen.common.PropertyKey
import com.typesafe.config.Config
import org.apache.spark.sql.{DataFrame, SparkSession}
import scala.util.{Failure, Success, Try}

/**
 * The common behavior for Delta Write
 */
private[etl] abstract class DeltaWriteActor[T] extends Actor { self: T =>
  //the options for loading the file
  @PropertyKey("options.*", false)
  protected var _options: Map[String, String] = Map.empty[String, String]

  //The columns separated by comma(,) used to partition data when writing.
  @PropertyKey("partitionBy", false)
  protected var _partitionBy: Option[String] = None

  //the sink type - path
  @PropertyKey("sinkPath", false)
  protected var _sinkPath: Option[String] = None
  //the value of the sink - either a directory path or table name
  @PropertyKey("sinkTable", false)
  protected var _sinkTable: Option[String] = None

  //the view from which to write
  @PropertyKey("view", true)
  protected var _view: Option[String] = None

  /**
   * Run the file-reader
   */
  def run(ctx: ExecutionContext)(implicit session: SparkSession): Option[DataFrame] = for {
    df <- this._view.flatMap(name => ctx.getView(name))
  } yield Try(write(df)) match {
    case Success(_) => df
    case Failure(ex) => throw new RuntimeException(s"Cannot write data to the target - ${this._sinkTable.getOrElse(this._sinkPath.getOrElse(""))}.", ex)
  }

  /**
   * Initialize the actor with the properties & config
   *
   * @param properties
   * @param config
   */
  override def init(properties: Seq[(String, String)], config: Config)(implicit session: SparkSession): Unit = {
    super.init(properties, config)

    validate(Seq(this._sinkTable, this._sinkPath), "The sinkTable & sinkPath in DeltaWriter/DeltaStreamWriter cannot be all empty.")
  }

  //write the dataframe
  protected def write(df: DataFrame): Unit

  /**
   * The load option
   */
  def readOption(name: String, value: String): T = { this._options = this._options + (name -> value); this }
  /**
   * The load options
   */
  def readOptions(opts: Map[String, String]): T = { this._options = this._options ++ opts; this }

  /**
   * Partition by
   */
  def partitionBy(column: String, columns: String*): T = { this._partitionBy = Some((Seq(column) ++ columns.toSeq).mkString(",")); this }

  /**
   * The table that writes to
   */
  def sinkTable(table: String): T = { this._sinkTable = Some(table); this }
  /**
   * The path that writes to
   */
  def sinkPath(path: String): T = { this._sinkPath = Some(path); this }

  /**
   * The source view for writing
   */
  def sourceView(view: String): T = { this._view = Some(view); this }
}
