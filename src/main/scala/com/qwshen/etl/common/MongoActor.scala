package com.qwshen.etl.common

import com.qwshen.common.PropertyKey

private[etl] abstract class MongoActor[T] extends Actor { self: T =>
  //the mongodb host
  @PropertyKey("host", true)
  protected var _host: Option[String] = None
  //the mongodb port
  @PropertyKey("port", true)
  protected var _port: Option[Int] = None

  //the database
  @PropertyKey("database", true)
  protected var _database: Option[String] = None
  //the collection
  @PropertyKey("collection", true)
  protected var _collection: Option[String] = None

  //the account-user
  @PropertyKey("user", false)
  protected var _user: Option[String] = None
  //the account-password
  @PropertyKey("password", false)
  protected var _password: Option[String] = None

  //db options
  @PropertyKey("options.*", false)
  protected var _options: Map[String, String] = Map.empty[String, String]

  /**
   * The host of mongodb-server
   * @param hostName
   * @return
   */
  def host(hostName: String): T = { this._host = Some(hostName); this }

  /**
   * The port of mongodb-server
   * @param portNum
   * @return
   */
  def port(portNum: Int): T = { this._port = Some(portNum); this }

  /**
   * The target database
   * @param dbName
   * @return
   */
  def database(dbName: String): T = { this._database = Some(dbName); this }

  /**
   * The target collection
   * @param collectionName
   * @return
   */
  def collection(collectionName: String): T = { this._collection = Some(collectionName); this }

  /**
   * The user-name for accessing target database
   * @param userName
   * @return
   */
  def user(userName: String): T = { this._user = Some(userName); this }

  /**
   * The password of the user for access ing target database
   * @param userPassword
   * @return
   */
  def password(userPassword: String): T = { this._password = Some(userPassword); this }

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
