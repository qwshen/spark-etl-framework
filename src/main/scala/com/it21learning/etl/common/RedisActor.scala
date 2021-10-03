package com.it21learning.etl.common

import com.it21learning.common.PropertyKey

/**
 * This is a common base class for Redis Reader & Writer
 * @tparam T
 */
private[etl] abstract class RedisActor[T] extends Actor { self: T =>
  //host of the redis
  @PropertyKey("host", true)
  protected var _host: Option[String] = None
  //port of the redis
  @PropertyKey("port", false)
  protected var _port: Option[String] = Some("6379")
  //port of the redis
  @PropertyKey("dbNum", true)
  protected var _dbNum: Option[String] = None
  //port of the redis
  @PropertyKey("dbTable", true)
  protected var _table: Option[String] = None

  //port of the redis
  @PropertyKey("authPassword", false)
  protected var _authPassword: Option[String] = None

  //db options
  @PropertyKey("options.*", false)
  protected var _options: Map[String, String] = Map.empty[String, String]

  /**
   * The host name of the redis
   * @param hostName
   * @return
   */
  def host(hostName: String): T = { this._host = Some(hostName); this }

  /**
   * The port @ othe redis
   * @param portNum
   * @return
   */
  def port(portNum: Int): T = { this._port = Some(portNum.toString); this }

  /**
   * The db # of the redis
   * @param num
   * @return
   */
  def dbNum(num: Int): T = { this._dbNum = Some(num.toString); this }

  /**
   * The table name to be written
   * @param tableName
   * @return
   */
  def table(tableName: String): T = { this._table = Some(tableName); this }

  /**
   * The password for authentication
   *
   * @param password
   * @return
   */
  def authPassword(password: String): T = { this._authPassword = Some(password); this }

  /**
   * Add one dbOption
   * @param name
   * @param value
   * @return
   */
  def option(name: String, value: String): T = { this._options = this._options + (name -> value); this}

  /**
   * Add multiple dbOptions
   * @param opts
   * @return
   */
  def options(opts: Map[String, String]): T = { this._options = this._options ++ opts; this }
}
