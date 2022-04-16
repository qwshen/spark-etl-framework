package com.qwshen.etl.common

import com.qwshen.common.PropertyKey

/**
 * The base class for Iceberg reader & writer
 * @tparam T
 */
abstract class IcebergActor[T] extends Actor { self: T =>
  //table - the full name of an iceberg table
  @PropertyKey("table", true)
  protected var _table: Option[String] = None

  //the options for loading the file
  @PropertyKey("options.*", false)
  protected var _options: Map[String, String] = Map.empty[String, String]

  /**
   * The location
   *
   * @param value
   * @return
   */
  def location(value: String): T = { this._table = Some(value); this }

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
