package com.it21learning.etl.common

import com.typesafe.config.Config
import org.apache.spark.sql.{DataFrame, SparkSession}
import scala.util.{Failure, Success, Try}
import scala.xml.NodeSeq

/**
 * The common behavior for Delta Write
 *
 * @tparam T
 */
private[etl] abstract class DeltaWriteActor[T] extends Actor { self: T =>
  //the options for loading the file
  protected var _options: Map[String, String] = Map.empty[String, String]

  //The columns separated by comma(,) used to partition data when writing.
  protected var _partitionBy: Seq[String] = Nil

  //the sink type - path or table
  protected var _sinkType: Option[String] = None
  //the value of the sink - either a directory path or table name
  protected var _sinkLocation: Option[String] = None

  //the view from which to write
  protected var _view: Option[String] = None

  /**
   * Run the file-reader
   *
   * @param ctx     - the execution context
   * @param session - the spark-session
   * @return
   */
  def run(ctx: ExecutionContext)(implicit session: SparkSession): Option[DataFrame] = for {
    _ <- validate(this._view, "The view in DeltaWriter/DeltaStreamWriter is mandatory.")
    df <- this._view.flatMap(name => ctx.getView(name))
  } yield Try(write(df)) match {
    case Success(_) => df
    case Failure(ex) => throw new RuntimeException(s"Cannot write data to the target - ${this._sinkLocation.getOrElse("")}.", ex)
  }

  //write the dataframe
  protected def write(df: DataFrame): Unit

  /**
   * Initialize the file reader from the xml definition
   *
   * @param config  - the configuration object
   * @param session - the spark-session object
   */
  override def init(definition: NodeSeq, config: Config)(implicit session: SparkSession): Unit = {
    super.init(config)

    //scan bootstrap-servers & topics first
    (definition \ "property").foreach(prop => (prop \ "@name").text match {
      case "options" => this._options = (prop \ "option").map(o => ((o \ "@name").text, (o \ "@value").text)).toMap[String, String]
      case "partitionBy" => this._partitionBy = prop.text.split(",")
      case "sink" => (prop \ "definition").headOption.foreach(v => (v \ "@name").text match {
        case "path" => {
          this._sinkType = Some("path")
          this._sinkLocation = Some((prop \ "definition").text)
        }
        case "table" => {
          this._sinkType = Some("table")
          this._sinkLocation = Some((prop \ "definition").text)
        }
        case _ =>
      })
      case "view" => this._view = Some(prop.text)
      case _ =>
    })

    validate(this._sinkType, "The sink-type in DeltaWriter/DeltaStreamWriter is mandatory or its value is invalid.", Seq("path", "table"))
    validate(this._sinkLocation, "The sinkLocation in DeltaWriter/DeltaStreamWriter is mandatory.")
    validate(this._view, "The view in DeltaWriter/DeltaStreamWriter is mandatory.")
  }

  /**
   * The load option
   *
   * @param name
   * @param value
   * @return
   */
  def readOption(name: String, value: String): T = { this._options = this._options + (name -> value); this }
  /**
   * The load options
   *
   * @param opts
   * @return
   */
  def readOptions(opts: Map[String, String]): T = { this._options = this._options ++ opts; this }

  /**
   * Partition by
   *
   * @param column
   * @param columns
   * @return
   */
  def partitionBy(column: String, columns: String*): T = { this._partitionBy = Seq(column) ++ columns.toSeq; this }

  /**
   * The table that writes to
   *
   * @param table
   * @return
   */
  def sinkTable(table: String): T = {
    this._sinkType = Some("table")
    this._sinkLocation = Some(table);
    this
  }
  /**
   * The path that writes to
   *
   * @param path
   * @return
   */
  def sinkPath(path: String): T = {
    this._sinkType = Some("path")
    this._sinkLocation = Some(path)
    this
  }

  /**
   * The source view for writing
   *
   * @param view
   * @return
   */
  def sourceView(view: String): T = { this._view = Some(view); this }
}
