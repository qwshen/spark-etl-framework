package com.qwshen.etl.source

import com.qwshen.common.PropertyKey
import com.qwshen.common.io.FileChannel
import com.qwshen.etl.common.{RedisActor, JobContext}
import com.typesafe.config.Config
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.functions.current_timestamp
import scala.util.{Failure, Success, Try}

/**
 * Stream Read from Redis. Note - the redis requires to be 5.0.5+.
 */
class RedisStreamReader extends RedisActor[RedisStreamReader] {
  @PropertyKey("ddlSchemaString", false)
  protected var _ddlSchemaString: Option[String] = None
  @PropertyKey("ddlSchemaFile", false)
  protected var _ddlSchemaFile: Option[String] = None

  //the schema of the target data-frame
  private var _schema: Option[StructType] = None

  //add timestamp
  @PropertyKey("addTimestamp", false)
  protected var _addTimestamp: Boolean = false

  //water-mark time field
  @PropertyKey("watermark.timeField", false)
  protected var _wmTimeField: Option[String] = None
  //water-mark delay duration
  @PropertyKey("watermark.delayThreshold", false)
  protected var _wmDelayThreshold: Option[String] = None

  /**
   * Initialize the file reader
   *
   * @param config - the configuration object
   * @param session - the spark-session object
   */
  override def init(properties: Seq[(String, String)], config: Config)(implicit session: SparkSession): Unit = {
    super.init(properties, config)

    //prepare schema
    if (this._schema.isEmpty) {
      this._schema = (if (this._ddlSchemaString.nonEmpty) this._ddlSchemaString else this._ddlSchemaFile.map(f => FileChannel.loadAsString(f)))
        .flatMap(ss => Try(StructType.fromDDL(ss)) match {
          case Success(s) => Some(s)
          case Failure(t) => throw new RuntimeException(s"The schema [$ss] is not in valid DDL format.", t)
        })
    }
  }

  /**
   * Run the jdbc-reader
   *
   * @param ctx - the execution context
   * @param session - the spark-session
   * @return
   */
  def run(ctx: JobContext)(implicit session: SparkSession): Option[DataFrame] = for {
    host <- this._host
    port <- this._port
    dbNum <- this._dbNum
    table <- this._table
  } yield Try {
    var options = Map("host" -> host, "port" -> port.toString, "dbNum" -> dbNum.toString, "table" -> table)
    this._authPassword match {
      case Some(pwd) => options = options + ("auth" -> pwd)
      case _ =>
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
