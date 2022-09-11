package com.qwshen.etl.common

import com.qwshen.common.PropertyKey

private[etl] abstract class FlightActor[T] extends Actor { self: T =>
  @PropertyKey("connection.host", true)
  protected var _host: Option[String] = None
  @PropertyKey("connection.port", true)
  protected var _port: Option[Int] = None
  @PropertyKey("connection.user", true)
  protected var _user: Option[String] = None
  @PropertyKey("connection.password", true)
  protected var _password: Option[String] = None
  @PropertyKey("connection.table", true)
  protected var _table: Option[String] = None

  //the database read/write options
  @PropertyKey("options.*", false)
  protected var _options: Map[String, String] = Map.empty[String, String]

  /**
   * The host name of the flight end-point
   * @param hostName
   * @return
   */
  def host(hostName: String): T = { this._host = Some(hostName); this }
  /**
   * The port number of the flight end-point
   * @param portNumber
   * @return
   */
  def port(portNumber: Int): T = { this._port = Some(portNumber); this }
  /**
   * The user for connection
   * @param user
   * @return
   */
  def user(userName: String): T = { this._user = Some(userName); this }
  /**
   * The user's password
   * @param userPassword
   * @return
   */
  def password(userPassword: String): T = { this._password = Some(userPassword); this }
  /**
   * The flight table
   * @param table
   * @return
   */
  def table(tableName: String): T = { this._table = Some(tableName); this }

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
  def pptions(opts: Map[String, String]): T = { this._options = this._options ++ opts; this }
}
