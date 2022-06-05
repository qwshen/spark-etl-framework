package com.qwshen.etl.source

import com.qwshen.common.PropertyKey
import com.qwshen.etl.common.{JobContext, FileReadActor}
import org.apache.spark.sql.functions.current_timestamp
import org.apache.spark.sql.{DataFrame, SparkSession}
import scala.util.{Failure, Success, Try}

/**
 * This loader is for loading json, avro & parquet files in streaming mode
 */
class FileStreamReader extends FileReadActor[FileStreamReader] {
  //water-mark time field
  @PropertyKey("watermark.timeField", false)
  protected var _wmTimeField: Option[String] = None
  //water-mark delay duration
  @PropertyKey("watermark.delayThreshold", false)
  protected var _wmDelayThreshold: Option[String] = None

  //add timestamp
  @PropertyKey("addTimestamp", false)
  protected var _addTimestamp: Boolean = false

  /**
   * Run the file-stream-reader
   *
   * @param ctx - the execution context
   * @param session - the spark-session
   * @return
   */
  override def run(ctx: JobContext)(implicit session: SparkSession): Option[DataFrame] = for {
    fmt <- this._format
    schema <- this._schema
    uri <- this._fileUri
  } yield Try {
    val df = this._options.foldLeft(session.readStream.format(fmt))((s, o) => s.option(o._1, o._2)).schema(schema).load(uri)
    val dfResult = if (this._addTimestamp) df.withColumn("__timestamp", current_timestamp) else df
    //enable water-mark if required
    (this._wmTimeField, this._wmDelayThreshold) match {
      case (Some(m), Some(t)) => dfResult.withWatermark(m, t)
      case _ => dfResult
    }
  } match {
    case Success(df) => df
    case Failure(ex) => throw new RuntimeException(s"Cannot load the file into data-frame - ${_fileUri}.", ex)
  }

  /**
   * Specify the water-mark time field
   *
   * @param field
   * @return
   */
  def watermarkTimeField(field: String): FileStreamReader = { this._wmTimeField = Some(field); this }

  /**
   * Specify teh water-mark delay threshold
   *
   * @param duration
   * @return
   */
  def watermarkDelayThreshold(duration: String): FileStreamReader = { this._wmDelayThreshold = Some(duration); this }

  /**
   * Flag of whether or not to add __timestamp with current timestamp.
   *
   * @param value
   * @return
   */
  def addTimestamp(value: Boolean = false): FileStreamReader = { this._addTimestamp = value; this }
}
