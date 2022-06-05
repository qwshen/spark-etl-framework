package com.qwshen.etl.source

import com.qwshen.etl.common.{DeltaReadActor, JobContext}
import org.apache.spark.sql.functions.current_timestamp
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.util.{Failure, Success, Try}
import com.qwshen.common.PropertyKey
import com.typesafe.config.Config

/**
 * This reader reads data from delta lake into a data-frame in streaming mode.
 */
class DeltaStreamReader extends DeltaReadActor[DeltaStreamReader] {
  //add timestamp
  @PropertyKey("addTimestamp", false)
  protected var _addTimestamp: Boolean = false

  @PropertyKey("watermark.timeField", false)
  protected var _wmTimeField: Option[String] = None
  //water-mark delay duration
  @PropertyKey("watermark.delayThreshold", false)
  protected var _wmDelayThreshold: Option[String] = None

  /**
   * Initialize the delta-stream-readers
   */
  override def init(properties: Seq[(String, String)], config: Config)(implicit session: SparkSession): Unit = {
    super.init(properties, config)

    this.validate(this._sourcePath, "The source-path is mandatory for delta stream readers")
  }

  /**
   * Execute the action
   *
   * @param ctx     - the execution context
   * @param session - the spark-session
   *  @return
   */
  def run(ctx: JobContext)(implicit session: SparkSession): Option[DataFrame] = for {
    path <- this._sourcePath
  } yield Try {
    //the initial DataframeReader
    val df = this._options.foldLeft(session.readStream.format("delta"))((r, o) => r.option(o._1, o._2)).load(path)

    //plug in the special __timestamp with current-timestamp
    val dfResult = if (this._addTimestamp) df.withColumn("__timestamp", current_timestamp) else df
    //enable water-mark if required
    (this._wmTimeField, this._wmDelayThreshold) match {
      case (Some(m), Some(t)) => dfResult.withWatermark(m, t)
      case _ => dfResult
    }
  } match {
    case Success(df) => df
    case Failure(t) => throw new RuntimeException(s"Load from delta $path failed.", t)
  }

  /**
   * Specify the water-mark time field
   *
   * @param field
   * @return
   */
  def watermarkTimeField(field: String): DeltaStreamReader = { this._wmTimeField = Some(field); this }

  /**
   * Specify teh water-mark delay threshold
   *
   * @param duration
   * @return
   */
  def watermarkDelayThreshold(duration: String): DeltaStreamReader = { this._wmDelayThreshold = Some(duration); this }

  /**
   * Flag of whether or not to add __timestamp with current timestamp.
   *
   * @param value
   * @return
   */
  def addTimestamp(value: Boolean = false): DeltaStreamReader = { this._addTimestamp = value; this }
}
