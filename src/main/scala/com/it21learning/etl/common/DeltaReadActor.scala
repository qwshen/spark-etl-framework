package com.it21learning.etl.common

import com.typesafe.config.Config
import org.apache.spark.sql.SparkSession
import scala.xml.NodeSeq

/**
 * Common behavior for delta read
 * @tparam T
 */
private[etl] abstract class DeltaReadActor[T] extends Actor { self: T =>
  //the options for loading the file
  protected var _options: Map[String, String] = Map.empty[String, String]

  //source type
  protected var _sourceType: Option[String] = None
  //source location
  protected var _sourceLocation: Option[String] = None

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
      case "source" => (prop \ "definition").headOption.foreach(v => (v \ "@name").text match {
        case "path" => this.sourcePath(v.text)
        case "table" => this.sourceTable(v.text)
      })
      case _ =>
    })

    validate(this._sourceType, "The sourceType in DeltaReader/DeltaStreamReader is mandatory or its value is invalid.", Seq("path", "table"))
    validate(this._sourceLocation, "The sourceLocation in DeltaReader/DeltaStreamReader is mandatory.")
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
   * The source path
   *
   * @param path
   * @return
   */
  def sourcePath(path: String): T = {
    this._sourceType = Some("path")
    this._sourceLocation = Some(path)
    this
  }

  /**
   * The source table
   *
   * @param table
   * @return
   */
  def sourceTable(table: String): T = {
    this._sourceType = Some("table")
    this._sourceLocation = Some(table)
    this
  }
}
