package com.it21learning.etl.sink

import com.it21learning.common.PropertyKey
import com.it21learning.etl.common.{Actor, ExecutionContext}
import com.typesafe.config.Config
import org.apache.spark.sql.{DataFrame, SparkSession}
import scala.util.{Failure, Success, Try}
import scala.xml.NodeSeq

/**
 * Write to Redis. Note - the redis requires to be 5.0.5+.
 *
 * The following is the definition in xml format
 * <actor type="com.it21learning.etl.sink.RedisWriter">
 *   <property name="redis">
 *     <definition name="host">localhost</definition>
 *     <definition name="port">6379</definition>
 *     <definition name="dbNum">11</definition>
 *     <definition name="auth">password</definition>
 *     <definition name="table">users</definition>
 *   </property>
 *   <property name="dbOptions">
 *     <!--
 *       The binary serializes dataframe-rows into binary format to handle the nested structure inside the dataframe.
 *       However this persistence limits the data access in Redis to only with spark
 *     -->
 *     <definition name="model">binary</definition>
 *     <!-- make sure the underlying data structures match persistence model -->
 *     <definition name="filter.keys.by.type">true</definition>
 *     <!-- the key column. if multiple columns forms the key, combine them before writing to Redis -->
 *     <definition name="key.column">user_id</definition>
 *     <!-- Time to live in seconds. Redis expires data after the ttl -->
 *     <definition name="ttl">72000</definition>
 *     <!-- the number of items to be grouped when iterating over underlying RDD partition -->
 *     <definition name="iterator.grouping.size"> 1600</definition>
 *     <!-- count option of SCAN command (used to iterate over keys) -->
 *     <definition name="scan.count">240</definition>
 *     <!-- maximum number of commands per pipeline (used to batch commands) -->
 *     <definition name="max.pipeline.size">160</definition>
 *     <!-- timeout in milli-seconds for connection -->
 *     <definition name="timeout>1600</definition>
 *   </property>
 *   <property name="mode">overwrite</property>
 *   <property name="view">events</property>
 * </actor>
 */
final class RedisWriter extends Actor {
  //host of the redis
  @PropertyKey("redis.host", false)
  private var _host: Option[String] = None
  //port of the redis
  @PropertyKey("redis.port", false)
  private var _port: Option[String] = Some("6379")
  //port of the redis
  @PropertyKey("redis.dbNum", false)
  private var _dbNum: Option[String] = None
  //port of the redis
  @PropertyKey("redis.table", false)
  private var _table: Option[String] = None

  //port of the redis
  @PropertyKey("redis.auth", false)
  private var _authPassword: Option[String] = None

  //db options
  @PropertyKey("dbOptions", true)
  private var _dbOptions: Map[String, Any] = Map.empty[String, Any]

  //mode
  @PropertyKey("mode", false)
  private var _mode: Option[String] = None
  //view
  @PropertyKey("view", false)
  private var _view: Option[String] = None

  /**
   * Initialize the redis reader/writer from the xml definition
   *
   * @param config     - the configuration object
   * @param session    - the spark-session object
   */
  override def init(definition: NodeSeq, config: Config)(implicit session: SparkSession): Unit = {
    super.init(definition, config)

    validate(this._host, "The host of the redis in RedisWriter is mandatory.")
    validate(this._dbNum, "The dbNum of the redis in RedisWriter is mandatory.")
    validate(this._table, "The table of the redis in RedisWriter is mandatory.")
    validate(this._mode, "The mode in RedisWriter is mandatory.")
    validate(this._view, "The view in RedisWriter is mandatory.")
  }

  /**
   * Run the jdbc-reader
   *
   * @param ctx - the execution context
   * @param session - the spark-session
   * @return
   */
  def run(ctx: ExecutionContext)(implicit session: SparkSession): Option[DataFrame] = for {
    _ <- validate(this._host, "The host of the redis in RedisWriter is mandatory.")
    host <- this._host
    _ <-validate(this._dbNum, "The dbNum of the redis in RedisWriter is mandatory.")
    port <- this._port
    dbNum <- this._dbNum
    _ <-validate(this._table, "The table of the redis in RedisWriter is mandatory.")
    table <- this._table
    _ <- validate(this._mode, "The mode in RedisWriter is mandatory.")
    mode <- this._mode
    _ <- validate(this._view, "The view in RedisWriter is mandatory.")
    df <- this._view.flatMap(name => ctx.getView(name))
  } yield Try {
    val options = this._authPassword match {
      case Some(pwd) => Map("host" -> host, "port" -> port.toString, "dbNum" -> dbNum.toString, "table" -> table, "auth" -> pwd)
      case _ => Map("host" -> host, "port" -> port.toString, "dbNum" -> dbNum.toString, "table" -> table)
    }
    (options ++ this._dbOptions).foldLeft(df.write.format("org.apache.spark.sql.redis"))((w, o) => w.option(o._1, o._2.toString)).mode(mode).save
  } match {
    case Success(_) => df
    case Failure(ex) => throw new RuntimeException(s"Cannot load from source - ${this._table}.", ex)
  }

  /**
   * The host name of the redis
   * @param hostName
   * @return
   */
  def host(hostName: String): RedisWriter = { this._host = Some(hostName); this }

  /**
   * The port @ othe redis
   * @param portNum
   * @return
   */
  def port(portNum: Int): RedisWriter = { this._port = Some(portNum.toString); this }

  /**
   * The db # of the redis
   * @param num
   * @return
   */
  def dbNum(num: Int): RedisWriter = { this._dbNum = Some(num.toString); this }

  /**
   * The table name to be written
   * @param tableName
   * @return
   */
  def table(tableName: String): RedisWriter = { this._table = Some(tableName); this }

  /**
   * The password for authentication
   *
   * @param password
   * @return
   */
  def authPassword(password: String): RedisWriter = { this._authPassword = Some(password); this }

  /**
   * Add one dbOption
   * @param name
   * @param value
   * @return
   */
  def dbOption(name: String, value: String): RedisWriter = { this._dbOptions = this._dbOptions + (name -> value); this}

  /**
   * Add multiple dbOptions
   * @param options
   * @return
   */
  def dbOptions(options: Map[String, String]): RedisWriter = { this._dbOptions = this._dbOptions ++ options; this }

  /**
   * The write mode
   * @param mode
   * @return
   */
  def writeMode(mode: String): RedisWriter = { this._mode = Some(mode); this }

  /**
   * The source view to be written
   * @param view
   * @return
   */
  def sourceView(view: String): RedisWriter = { this._view = Some(view); this }
}
