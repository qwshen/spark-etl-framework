package com.qwshen.etl.common

import com.typesafe.config.Config
import org.apache.spark.sql.SparkSession
import com.qwshen.common.PropertyKey

/**
 * Common behavior for delta read
 */
private[etl] abstract class DeltaReadActor[T] extends Actor { self: T =>
  //the options for loading the file
  @PropertyKey("options.*", false)
  protected var _options: Map[String, String] = Map.empty[String, String]

  //source table
  @PropertyKey("sourceTable", false)
  protected var _sourceTable: Option[String] = None
  //source location
  @PropertyKey("sourcePath", false)
  protected var _sourcePath: Option[String] = None

  /**
   * Initialize the actor with the properties & config
   *
   * @param properties
   * @param config
   */
  override def init(properties: Seq[(String, String)], config: Config)(implicit session: SparkSession): Unit = {
    super.init(properties, config)

    validate(Seq(this._sourceTable, this._sourcePath), "The sourceTable & sourcePath in DeltaReader/DeltaStreamReader cannot be all empty.")
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
  def sourcePath(path: String): T = { this._sourcePath = Some(path); this }

  /**
   * The source table
   *
   * @param table
   * @return
   */
  def sourceTable(table: String): T = { this._sourceTable = Some(table); this }
}
