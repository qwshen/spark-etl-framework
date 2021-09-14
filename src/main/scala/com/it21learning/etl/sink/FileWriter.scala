package com.it21learning.etl.sink

import com.it21learning.common.PropertyKey
import com.it21learning.etl.common.{Actor, ExecutionContext}
import com.typesafe.config.Config
import org.apache.spark.sql.{DataFrame, SparkSession}
import scala.util.{Failure, Success, Try}
import scala.xml.NodeSeq

/**
 * This writer is for writing csv, json, avro & parquet files to the target location
 */
final class FileWriter extends Actor {
  //the file format
  @PropertyKey("format", true)
  private var _format: Option[String] = None

  //the options for loading the file
  @PropertyKey("options.*", false)
  private var _options: Map[String, String] = Map.empty[String, String]

  //partition-by: the columns separated by comma(,) used to partition data when writing.
  @PropertyKey("partitionBy", false)
  private var _partitionBy: Option[String] = None

  //the mode for the writing
  @PropertyKey("mode", false)
  private var _mode: Option[String] = None
  //the file location
  @PropertyKey("fileUri", true)
  private var _fileUri: Option[String] = None

  //the view for writing out
  @PropertyKey("view", false)
  private var _view: Option[String] = None

  /**
   * Run the file-writer
   *
   * @param ctx - the execution context
   * @param session - the spark-session
   * @return
   */
  def run(ctx: ExecutionContext)(implicit session: SparkSession): Option[DataFrame] = for {
    fmt <- this._format
    mode <- this._mode
    uri <- this._fileUri
    df <- this._view.flatMap(name => ctx.getView(name))
  } yield Try {
    val initWriter = this._options.foldLeft(df.write.format(fmt))((s, o) => s.option(o._1, o._2))
    //with partitionBy
    val partitionWriter = this._partitionBy.foldLeft(initWriter)((w, cs) => w.partitionBy(cs.split(","): _*))
    //write
    partitionWriter.mode(mode).save(uri)
  } match {
    case Success(_) => df
    case Failure(ex) => throw new RuntimeException(s"Cannot write data to the target - $uri.", ex)
  }

  /**
   * Initialize the file writer
   *
   * @param config - the configuration object
   * @param session - the spark-session object
   */
  override def init(definition: NodeSeq, config: Config)(implicit session: SparkSession): Unit = {
    super.init(definition, config)

    validate[String](this._format, "The format in FileWriter is mandatory.", Seq("csv", "json", "parquet", "avro"))
    validate[String](this._mode, "The write-mode in FileWriter is mandatory.", Seq("append", "overwrite"))
    validate[String](this._fileUri, "The path in FileWriter is mandatory.")
    validate[String](this._view, "The view in FileWriter is mandatory.")
  }

  /**
   * The format of the source files.
   *
   * @param format
   * @return
   */
  def targetFormat(format: String): FileWriter = { this._format = Some(format); this }

  /**
   * The load option
   *
   * @param name
   * @param value
   * @return
   */
  def writeOption(name: String, value: String): FileWriter = { this._options = this._options + (name -> value); this }

  /**
   * The load options
   *
   * @param options
   * @return
   */
  def writeOptions(options: Map[String, String]): FileWriter = { this._options = this._options ++ options; this }

  /**
   * Partition by
   *
   * @param column
   * @param columns
   * @return
   */
  def partitionBy(column: String, columns: String*): FileWriter = { this._partitionBy = Some((Seq(column) ++ columns.toSeq).mkString(",")); this }

  /**
   * The source path of the files
   *
   * @param path
   * @return
   */
  def targetPath(path: String): FileWriter = { this._fileUri = Some(path); this }

  /**
   * The write mode. The valid values are: append, overwrite.
   *
   * @param value
   * @return
   */
  def mode(value: String): FileWriter = { this._mode = Some(value); this }

  /**
   * The view of its data to be written out
   *
   * @param view
   * @return
   */
  def sourceView(view: String): FileWriter = { this._view = Some(view); this }
}
