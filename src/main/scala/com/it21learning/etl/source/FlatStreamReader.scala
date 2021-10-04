package com.it21learning.etl.source

import com.it21learning.common.PropertyKey
import com.it21learning.etl.common.{ExecutionContext, FlatReadActor}
import org.apache.spark.sql.{DataFrame, SparkSession}
import scala.util.{Failure, Success, Try}
import org.apache.spark.sql.functions.current_timestamp

/**
 * To load a text file in streaming mode.
 *
 * The output dataframe has the following columns:
 *   - row_value: the content of each row
 *   - row_no: the sequence number of each row.
 */
final class FlatStreamReader extends FlatReadActor[FlatStreamReader] {
  //water-mark time field
  @PropertyKey("watermark.timeField", false)
  private var _wmTimeField: Option[String] = None
  //water-mark delay duration
  @PropertyKey("watermark.delayThreshold", false)
  private var _wmDelayThreshold: Option[String] = None

  //add timestamp
  @PropertyKey("addTimestamp", false)
  private var _addTimestamp: Boolean = false

  /**
   * Run the file-reader
   *
   * @param ctx - the execution context
   * @param session - the spark-session
   * @return
   */
  override def run(ctx: ExecutionContext)(implicit session: SparkSession): Option[DataFrame] = for {
    uri <- this._fileUri
  } yield Try {
    import session.implicits._
    val dfInit = this._options.foldLeft(session.readStream.format("text"))((r, o) => r.option(o._1, o._2)).load(uri).as[String]

    val getType = (name: String) => this._schema.flatMap(_.fields.find(_.name.equals(name)).map(x => x.dataType)).getOrElse(
      throw new RuntimeException(s"The data type is unknown for column - $name")
    )
    val df = this._format match {
      case Seq(_, _ @ _*) => dfInit.select(this._format.map(f => $"value".substr(f.startPos, f.length).as(f.name).cast(getType(f.name))): _*)
      case _ => this._valueField match {
          case Some(value) => dfInit.withColumnRenamed("value", value)
          case _ => dfInit.withColumnRenamed("value", "row_value")
        }
    }

    val dfResult = if (this._addTimestamp) df.withColumn("__timestamp", current_timestamp) else df
    //enable water-mark if required
    (this._wmTimeField, this._wmDelayThreshold) match {
      case (Some(m), Some(t)) => dfResult.withWatermark(m, t)
      case _ => dfResult
    }
  } match {
    case Success(df) => df
    case Failure(ex) => throw new RuntimeException(s"Cannot load the flat file - $uri", ex)
  }

  /**
   * Specify the water-mark time field
   *
   * @param field
   * @return
   */
  def watermarkTimeField(field: String): FlatStreamReader = { this._wmTimeField = Some(field); this }

  /**
   * Specify teh water-mark delay threshold
   *
   * @param duration
   * @return
   */
  def watermarkDelayThreshold(duration: String): FlatStreamReader = { this._wmDelayThreshold = Some(duration); this }

  /**
   * Flag of whether or not to add __timestamp with current timestamp.
   *
   * @param value
   * @return
   */
  def addTimestamp(value: Boolean = false): FlatStreamReader = { this._addTimestamp = value; this }
}
