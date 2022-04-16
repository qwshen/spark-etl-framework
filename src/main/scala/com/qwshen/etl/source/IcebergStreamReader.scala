package com.qwshen.etl.source

import com.qwshen.common.PropertyKey
import com.qwshen.etl.common.{ExecutionContext, IcebergActor}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.current_timestamp
import scala.util.{Failure, Success, Try}

/**
 * This class is to read iceberg tables in streaming mode.
 */
class IcebergStreamReader extends IcebergActor[IcebergStreamReader] {
  //add timestamp
  @PropertyKey("addTimestamp", false)
  protected var _addTimestamp: Boolean = false

  @PropertyKey("watermark.timeField", false)
  protected var _wmTimeField: Option[String] = None
  //water-mark delay duration
  @PropertyKey("watermark.delayThreshold", false)
  protected var _wmDelayThreshold: Option[String] = None

  /**
   * Execute the action
   *
   * @param ctx     - the execution context
   * @param session - the spark-session
   *  @return
   */
  def run(ctx: ExecutionContext)(implicit session: SparkSession): Option[DataFrame] = for {
    table <- this._table
  } yield Try {
    //the initial DataframeReader
    val df = this._options.foldLeft(session.readStream.format("iceberg"))((r, o) => r.option(o._1, o._2)).load(table)

    //plug in the special __timestamp with current-timestamp
    val dfResult = if (this._addTimestamp) df.withColumn("__timestamp", current_timestamp) else df

    //enable water-mark if required
    (this._wmTimeField, this._wmDelayThreshold) match {
      case (Some(m), Some(t)) => dfResult.withWatermark(m, t)
      case _ => dfResult
    }
  } match {
    case Success(df) => df
    case Failure(t) => throw new RuntimeException(s"Load from iceberg table [$table] failed.", t)
  }

  /**
   * Specify the water-mark time field
   *
   * @param field
   * @return
   */
  def watermarkTimeField(field: String): IcebergStreamReader = { this._wmTimeField = Some(field); this }

  /**
   * Specify teh water-mark delay threshold
   *
   * @param duration
   * @return
   */
  def watermarkDelayThreshold(duration: String): IcebergStreamReader = { this._wmDelayThreshold = Some(duration); this }

  /**
   * Flag of whether or not to add __timestamp with current timestamp.
   *
   * @param value
   * @return
   */
  def addTimestamp(value: Boolean = false): IcebergStreamReader = { this._addTimestamp = value; this }
}
