package com.qwshen.etl.common

import com.qwshen.common.PropertyKey
import com.typesafe.config.Config
import org.apache.spark.sql.SparkSession

/**
 * The base class for Iceberg reader & writer
 * @tparam T
 */
abstract class IcebergActor[T] extends Actor { self: T =>
  //location - either a table name or directory path
  @PropertyKey("location", true)
  protected var _location: Option[String] = None

  //the options for loading the file
  @PropertyKey("options.*", false)
  protected var _options: Map[String, String] = Map.empty[String, String]

  /**
   * The location
   *
   * @param value
   * @return
   */
  def location(value: String): T = { this._location = Some(value); this }

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
   * @param options
   * @return
   */
  def options(options: Map[String, String]): T = { this._options = this._options ++ options; this }
}
