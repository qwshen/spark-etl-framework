package com.it21learning.etl.source

import com.it21learning.etl.common.{DeltaReadActor, ExecutionContext}
import org.apache.spark.sql.functions.current_timestamp
import org.apache.spark.sql.{DataFrame, SparkSession}
import scala.util.{Failure, Success, Try}
import com.it21learning.common.PropertyKey

/**
 * This reader reads data from delta lake into a data-frame in streaming mode.
 */
final class DeltaStreamReader extends DeltaReadActor[DeltaStreamReader] {
  //add timestamp
  @PropertyKey("addTimestamp", false)
  private var _addTimestamp: Boolean = false

  @PropertyKey("watermark.timeField", false)
  private var _wmTimeField: Option[String] = None
  //water-mark delay duration
  @PropertyKey("watermark.delayThreshold", false)
  private var _wmDelayThreshold: Option[String] = None

  /**
   * Execute the action
   *
   * @param ctx     - the execution context
   * @param session - the spark-session
   *  @return
   */
  def run(ctx: ExecutionContext)(implicit session: SparkSession): Option[DataFrame] = Try {
    //the initial DataframeReader
    val reader = this._options.foldLeft(session.readStream.format("delta"))((r, o) => r.option(o._1, o._2))
    //load
    val df = (this._sourceTable, this._sourcePath) match {
      case (Some(table), _) =>
        //reader.table(table)
        throw new RuntimeException("As of Delta 1.0.0 by 24 May, 2020, it doesn't support stream-reading from table.")
      case (_, Some(path)) =>  reader.load(path)
      case _ => throw new RuntimeException("The source table andr source path are defined.")
    }

    //plug in the special __timestamp with current-timestamp
    val dfResult = if (this._addTimestamp) df.withColumn("__timestamp", current_timestamp) else df
    //enable water-mark if required
    (this._wmTimeField, this._wmDelayThreshold) match {
      case (Some(m), Some(t)) => dfResult.withWatermark(m, t)
      case _ => dfResult
    }
  } match {
    case Success(df) => Some(df)
    case Failure(t) => throw new RuntimeException(s"Load from delta ${this._sourceTable.getOrElse(this._sourcePath.getOrElse(""))} failed.", t)
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
