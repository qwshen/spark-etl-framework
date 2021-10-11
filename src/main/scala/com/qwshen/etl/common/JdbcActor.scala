package com.qwshen.etl.common

import com.qwshen.common.PropertyKey
import com.typesafe.config.Config
import org.apache.spark.sql.SparkSession

/**
 * The common base class for jdbc reade/write.
 */
private[etl] abstract class JdbcActor[T] extends Actor { self: T =>
  //information for connecting to target database
  @PropertyKey("connection.*", true)
  protected var _connection: Map[String, String] = Map.empty[String, String]

  //the database read/write options
  @PropertyKey("options.*", false)
  protected var _options: Map[String, String] = Map.empty[String, String]

  /**
   * Initialize the kafka reader
   */
  override def init(properties: Seq[(String, String)], config: Config)(implicit session: SparkSession): Unit = {
    super.init(properties, config)

    //check if driver & url are specified
    this.validate(this._connection, Seq("driver", "url", "dbtable"), "The driver, url & dbtable are mandatory for connecting to database.")
  }

  /**
   * The jdbc driver
   * @param driver
   * @return
   */
  def dbDriver(driver: String): T = { this._connection = this._connection + ("driver" -> driver); this }

  /**
   * The jdbc url
   * @param url
   * @return
   */
  def dbUrl(url: String): T = { this._connection = this._connection + ("url" -> url); this }

  /**
   * The jdbc table
   * @param table
   * @return
   */
  def dbTable(table: String): T = { this._connection = this._connection + ("dbtable" -> table); this }

  /**
   * The user for connection
   * @param user
   * @return
   */
  def dbUser(user: String): T = { this._connection = this._connection + ("user" -> user); this }

  /**
   * The user's password
   * @param password
   * @return
   */
  def dbPassword(password: String): T = { this._connection = this._connection + ("password" -> password); this }

  /**
   * The load option
   *
   * @param name
   * @param value
   * @return
   */
  def dbOption(name: String, value: String): T = { this._options = this._options + (name -> value); this }
  /**
   * The load options
   *
   * @param opts
   * @return
   */
  def dbOptions(opts: Map[String, String]): T = { this._options = this._options ++ opts; this }
}
