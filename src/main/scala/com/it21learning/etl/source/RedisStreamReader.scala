package com.it21learning.etl.source

import com.it21learning.common.PropertyKey
import com.it21learning.common.io.FileChannel
import com.it21learning.etl.common.{Actor, ExecutionContext}
import com.typesafe.config.Config
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.functions.current_timestamp
import scala.util.{Failure, Success, Try}
import scala.xml.NodeSeq

/**
 * Stream Read from Redis. Note - the redis requires to be 5.0.5+.
 *
 * The following is the definition in xml format
 * <actor type="com.it21learning.etl.source.RedisStreamReader">
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
 *     <!--
 *       the stream-keys (separated by comma if multiple keys specified) used for paralleling the read.
 *       Each key creates a spark partition.
 *       Note: for the parallel read, it is normally with either multiple stream-keys or single stream-key with multiple consumers
 *     -->
 *     <definition name="stream.keys">user-us,users-ca</definition>
 *     <!-- the number of consumers in parallel per stream-key. Each consumer maps to one spark partition -->
 *     <definition name="stream.parallelism">3</definition>
 *     <!-- the consumer group name. If not specified, the default consumer group is spark-source -->
 *     <definition name="stream.group.name">users</definition>
 *     <!-- the prefix of names of the consumers -->
 *     <definition name="stream.consumer.prefix">users</definition>
 *     <!--
 *       the offsets where starts the read. Please note that the offsets are per stream-key/consumer-group.
 *       To start from the beginning, set the offset id to 0-0.
 *     -->
 *     <definition name="stream.offsets">{"offsets":{"stream-key-name":{"consumer-group-name":"redis-source","offset":"1548083485360-0"}}}</definition>
 *     <!-- the number of items per batch to read from Redis -->
 *     <definition name="stream.read.batch.size">300</definition>
 *     <!-- the max time in milli-seconds for reading block -->
 *     <definition name="stream.read.block">1600</definition>
 *   </property>
 *   <property name="ddlSchema">
 *     <!-- schema can be embedded here or defined in a file. If both specified, the embedded version is used -->
 *     <definition name="string">id int, name string, amount float ...</definition>
 *     <definition name="file">schema/users.ddl</definition>
 *   </property>
 *   <!-- the watermark for the streaming read. -->
 *   <property> name="waterMark">
 *     <definition name="timeField">event_time</definition>
 *     <definition name="delayThreshold">10 seconds</definition>
 *   </property>
 *   <!-- to add an additional timestamp in the name of __timestamp -->
 *   <property name="addTimestamp">false</property>
 * </actor>
 */
final class RedisStreamReader extends Actor {
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

  //add timestamp
  @PropertyKey("addTimestamp", false)
  private var _addTimestamp: Boolean = false

  //water-mark time field
  @PropertyKey("waterMark.timeField", false)
  private var _wmTimeField: Option[String] = None
  //water-mark delay duration
  @PropertyKey("waterMark.delayThreshold", false)
  private var _wmDelayThreshold: Option[String] = None

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
    validate(this._host, "The host of the redis in RedisStreamReader is mandatory.")
    validate(this._dbNum, "The dbNum of the redis in RedisStreamReader is mandatory.")
    validate(this._table, "The table of the redis in RedisStreamReader is mandatory.")
  }

  /**
   * Run the jdbc-reader
   *
   * @param ctx - the execution context
   * @param session - the spark-session
   * @return
   */
  def run(ctx: ExecutionContext)(implicit session: SparkSession): Option[DataFrame] = for {
    _ <- validate(this._host, "The host of the redis in RedisStreamReader is mandatory.")
    host <- this._host
    _ <-validate(this._dbNum, "The dbNum of the redis in RedisStreamReader is mandatory.")
    port <- this._port
    dbNum <- this._dbNum
    _ <-validate(this._table, "The table of the redis in RedisStreamReader is mandatory.")
    table <- this._table
  } yield Try {
    val options = this._authPassword match {
      case Some(pwd) => Map("host" -> host, "port" -> port.toString, "dbNum" -> dbNum.toString, "table" -> table, "auth" -> pwd)
      case _ => Map("host" -> host, "port" -> port.toString, "dbNum" -> dbNum.toString, "table" -> table)
    }
    val df = this._schema.foldLeft((options ++ this._options)
      .foldLeft(session.readStream.format("redis"))((s, o) => s.option(o._1, o._2.toString)))((r, s) => r.schema(s)).load

    //plug in the special __timestamp with current-timestamp
    val dfResult = if (this._addTimestamp) df.withColumn("__timestamp", current_timestamp) else df
    //enable water-mark if required
    (this._wmTimeField, this._wmDelayThreshold) match {
      case (Some(m), Some(t)) => dfResult.withWatermark(m, t)
      case _ => dfResult
    }
  } match {
    case Success(df) => df
    case Failure(ex) => throw new RuntimeException(s"Cannot load from source - ${this._table}.", ex)
  }

  /**
   * The host name of the redis
   * @param hostName
   * @return
   */
  def host(hostName: String): RedisStreamReader = { this._host = Some(hostName); this }

  /**
   * The port @ othe redis
   * @param portNum
   * @return
   */
  def port(portNum: Int): RedisStreamReader = { this._port = Some(portNum.toString); this }

  /**
   * The db # of the redis
   * @param num
   * @return
   */
  def dbNum(num: Int): RedisStreamReader = { this._dbNum = Some(num.toString); this }

  /**
   * The table name to be written
   * @param tableName
   * @return
   */
  def table(tableName: String): RedisStreamReader = { this._table = Some(tableName); this }

  /**
   * The password for authentication
   *
   * @param password
   * @return
   */
  def authPassword(password: String): RedisStreamReader = { this._authPassword = Some(password); this }

  /**
   * Add one dbOption
   * @param name
   * @param value
   * @return
   */
  def option(name: String, value: String): RedisStreamReader = { this._options = this._options + (name -> value); this}

  /**
   * Add multiple dbOptions
   * @param opts
   * @return
   */
  def options(opts: Map[String, String]): RedisStreamReader = { this._options = this._options ++ opts; this }

  /**
   * The schema of the target data-frame
   *
   * @param schema
   * @return
   */
  def ddlSchema(schema: StructType): RedisStreamReader = { this._schema = Some(schema); this }

  /**
   * Specify the water-mark time field
   *
   * @param field
   * @return
   */
  def watermarkTimeField(field: String): RedisStreamReader = { this._wmTimeField = Some(field); this }

  /**
   * Specify teh water-mark delay threshold
   *
   * @param duration
   * @return
   */
  def watermarkDelayThreshold(duration: String): RedisStreamReader = { this._wmDelayThreshold = Some(duration); this }

  /**
   * Flag of whether or not to add __timestamp with current timestamp.
   *
   * @param value
   * @return
   */
  def addTimestamp(value: Boolean = false): RedisStreamReader = { this._addTimestamp = value; this }
}
