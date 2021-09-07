package com.it21learning.etl.source

import com.it21learning.common.PropertyKey
import com.it21learning.common.io.FileChannel
import com.it21learning.etl.common.{Actor, ExecutionContext}
import com.typesafe.config.Config
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, SparkSession}
import scala.util.{Failure, Success, Try}
import scala.xml.NodeSeq

/**
 * Read from Redis. Note - the redis requires to be 5.0.5+.
 *
 * The following is the definition in xml format
 * <actor type="com.it21learning.etl.source.RedisReader">
 *   <property name="redis">
 *     <definition name="host">localhost</definition>
 *     <definition name="port">6379</definition>
 *     <definition name="dbNum">11</definition>
 *     <definition name="auth">password</definition>
 *     <definition name="table">users</definition>
 *   </property>
 *   <property name="options">
 *     <!-- the key column. if multiple columns forms the key, combine them before  -->
 *     <definition name="key.column">user_id</definition>
 *     <!-- match the keys based on the pattern -->
 *     <definition name="key.pattern">person*</definition>
 *     <!-- automatically infer the schema based on a random row -->
 *     <definition name="infer.schema">true</definition>
 *     <!-- number of partitions -->
 *     <definition name="partitions.number">16</definition>
 *     <!-- maximum number of commands per pipeline (used to batch commands) -->
 *     <definition name="max.pipeline.size">160</definition>
 *     <!-- count option of SCAN command (used to iterate over keys) -->
 *     <definition name="scan.count">240</definition>
 *     <!-- the number of items to be grouped when iterating over underlying RDD partition -->
 *     <definition name="iterator.grouping.size"> 1600</definition>
 *     <!-- timeout in milli-seconds for connection -->
 *     <definition name="timeout">1600</definition>
 *   </property>
 *   <property name="ddlSchema">
 *     <!-- schema can be embedded here or defined in a file. If both specified, the embedded version is used -->
 *     <definition name="string">id int, name string, amount float ...</definition>
 *     <definition name="file">schema/users.ddl</definition>
 *   </property>
 * </actor>
 */
final class RedisReader extends Actor {
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
  @PropertyKey("options", true)
  private var _options: Map[String, Any] = Map.empty[String, Any]

  //the schema of the target data-frame
  private var _schema: Option[StructType] = None

  /**
   * Initialize the redis reader/writer from the xml definition
   *
   * @param config     - the configuration object
   * @param session    - the spark-session object
   */
  override def init(definition: NodeSeq, config: Config)(implicit session: SparkSession): Unit = {
    super.init(definition, config)

    //check schema
    (this.getProperty[String]("ddlSchema.string"), this.getProperty[String]("ddlSchema.file")) match {
      case (Some(ss), _) => this._schema = Try(StructType.fromDDL(ss)) match {
        case Success(s) => Some(s)
        case Failure(t) => throw new RuntimeException(s"The schema [$ss] is not in valid DDL format.", t)
      }
      case (_, Some(fs)) => this._schema = Try(StructType.fromDDL(FileChannel.loadAsString(fs))) match {
        case Success(s) => Some(s)
        case Failure(t) => throw new RuntimeException(s"The schema is not in valid the DDL format - $fs", t)
      }
      case _ =>
    }
    validate(this._host, "The host of the redis in RedisReader is mandatory.")
    validate(this._dbNum, "The dbNum of the redis in RedisReader is mandatory.")
    validate(this._table, "The table of the redis in RedisReader is mandatory.")
  }

  /**
   * Run the jdbc-reader
   *
   * @param ctx - the execution context
   * @param session - the spark-session
   * @return
   */
  def run(ctx: ExecutionContext)(implicit session: SparkSession): Option[DataFrame] = for {
    _ <- validate(this._host, "The host of the redis in Redis reader/writer is mandatory.")
    host <- this._host
    _ <-validate(this._dbNum, "The dbNum of the redis in Redis reader/writer is mandatory.")
    port <- this._port
    dbNum <- this._dbNum
    _ <-validate(this._table, "The table of the redis in Redis reader/writer is mandatory.")
    table <- this._table
  } yield Try {
    val options = this._authPassword match {
      case Some(pwd) => Map("host" -> host, "port" -> port.toString, "dbNum" -> dbNum.toString, "table" -> table, "auth" -> pwd)
      case _ => Map("host" -> host, "port" -> port.toString, "dbNum" -> dbNum.toString, "table" -> table)
    }
    this._schema.foldLeft((options ++ this._options)
      .foldLeft(session.read.format("org.apache.spark.sql.redis"))((s, o) => s.option(o._1, o._2.toString)))((r, s) => r.schema(s)).load
  } match {
    case Success(df) => df
    case Failure(ex) => throw new RuntimeException(s"Cannot load from source - ${this._table}.", ex)
  }

  /**
   * The host name of the redis
   * @param hostName
   * @return
   */
  def host(hostName: String): RedisReader = { this._host = Some(hostName); this }

  /**
   * The port @ othe redis
   * @param portNum
   * @return
   */
  def port(portNum: Int): RedisReader = { this._port = Some(portNum.toString); this }

  /**
   * The db # of the redis
   * @param num
   * @return
   */
  def dbNum(num: Int): RedisReader = { this._dbNum = Some(num.toString); this }

  /**
   * The table name to be written
   * @param tableName
   * @return
   */
  def table(tableName: String): RedisReader = { this._table = Some(tableName); this }

  /**
   * The password for authentication
   *
   * @param password
   * @return
   */
  def authPassword(password: String): RedisReader = { this._authPassword = Some(password); this }

  /**
   * Add one dbOption
   * @param name
   * @param value
   * @return
   */
  def option(name: String, value: String): RedisReader = { this._options = this._options + (name -> value); this}

  /**
   * Add multiple dbOptions
   * @param opts
   * @return
   */
  def options(opts: Map[String, String]): RedisReader = { this._options = this._options ++ opts; this }

  /**
   * The schema of the target data-frame
   *
   * @param schema
   * @return
   */
  def ddlSchema(schema: StructType): RedisReader = { this._schema = Some(schema); this }
}
