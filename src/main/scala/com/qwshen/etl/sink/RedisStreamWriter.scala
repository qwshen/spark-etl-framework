package com.qwshen.etl.sink

import com.qwshen.common.PropertyKey
import com.qwshen.etl.common.{RedisActor, ExecutionContext}
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.{DataFrame, SparkSession}
import scala.util.{Failure, Success, Try}

/**
 * Stream-Write to Redis. Note - the redis requires to be 5.0.5+.
 */
final class RedisStreamWriter extends RedisActor[RedisStreamWriter] {
  //trigger mode
  @PropertyKey("trigger.mode", true)
  private var _triggerMode: Option[String] = None
  //trigger interval
  @PropertyKey("trigger.interval", true)
  private var _triggerInterval: Option[String] = None

  //the output mode
  @PropertyKey("outputMode", true)
  private var _outputMode: Option[String] = None
  //wait time in ms for test
  @PropertyKey("test.waittimeMS", false)
  private var _waittimeInMs: Option[Long] = None

  //view
  @PropertyKey("view", true)
  private var _view: Option[String] = None

  /**
   * Run the jdbc-reader
   *
   * @param ctx - the execution context
   * @param session - the spark-session
   * @return
   */
  def run(ctx: ExecutionContext)(implicit session: SparkSession): Option[DataFrame] = for {
    host <- this._host
    port <- this._port
    dbNum <- this._dbNum
    table <- this._table
    mode <- this._outputMode
    _ <- validate(this._options, Seq("checkpointLocation"), "The checkpoingLocation option is mandatory for Redis stream writing")
    checkpointLocation <- this._options.get("checkpointLocation")
    df <- this._view.flatMap(name => ctx.getView(name))
  } yield Try {
    var options = Map("host" -> host, "port" -> port.toString, "dbNum" -> dbNum.toString, "table" -> table)
    this._authPassword match {
      case Some(pwd) => options = options + ("auth" -> pwd)
      case _ =>
    }
    options = options ++ this._options.filter(!_._1.equals("checkpointLocation"))
    val writer = (df: DataFrame, id: Long) => options
      .foldLeft(df.write.format("org.apache.spark.sql.redis"))((w, o) => w.option(o._1, o._2.toString)).mode("append").save

    val streamQuery = df.writeStream.option("checkLocation", checkpointLocation).outputMode(mode).foreachBatch { writer }
    val triggerQuery = (this._triggerMode, this._triggerInterval) match {
      case (Some(m), Some(t)) if (m == "continuous") => streamQuery.trigger(Trigger.Continuous(t))
      case (Some(m), Some(t)) if (m == "processingTime") => streamQuery.trigger(Trigger.ProcessingTime(t))
      case (Some(m), _) if (m == "once") => streamQuery.trigger(Trigger.Once())
      case _ => streamQuery
    }
    this._waittimeInMs match {
      case Some(ts) => triggerQuery.start.awaitTermination(ts)
      case _ => triggerQuery.start.awaitTermination()
    }
  } match {
    case Success(_) => df
    case Failure(ex) => throw new RuntimeException(s"Cannot load from source - ${this._table}.", ex)
  }

  /**
   * The trigger mode
   *
   * @param mode
   * @return
   */
  def triggerMode(mode: String): RedisStreamWriter = { this._triggerMode = Some(mode); this }

  /**
   * The trigger interval
   * @param duration
   * @return
   */
  def triggerInterval(duration: String): RedisStreamWriter = { this._triggerInterval = Some(duration); this }

  /**
   * The output mode
   *
   * @param mode
   * @return
   */
  def outputMode(mode: String): RedisStreamWriter = { this._outputMode = Some(mode); this }

  /**
   * Wait time for streaming to execute before shut it down
   *
   * @param waittime
   * @return
   */
  def waitTimeInMs(waittime: Long): RedisStreamWriter = { this._waittimeInMs = Some(waittime); this }

  /**
   * The source view to be written
   * @param view
   * @return
   */
  def sourceView(view: String): RedisStreamWriter = { this._view = Some(view); this }
}
