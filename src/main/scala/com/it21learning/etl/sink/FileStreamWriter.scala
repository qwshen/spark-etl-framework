package com.it21learning.etl.sink

import com.it21learning.common.PropertyKey
import com.it21learning.etl.common.{ExecutionContext, FileWriteActor}
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.{DataFrame, SparkSession}
import scala.util.{Failure, Success, Try}

/**
 * This writer is for writing csv, json, avro & parquet files to the target location
 */
final class FileStreamWriter extends FileWriteActor[FileStreamWriter] {
  //output mode
  @PropertyKey("outputMode", true)
  private var _outputMode: Option[String] = None

  //trigger mode
  @PropertyKey("trigger.mode", false)
  private var _triggerMode: Option[String] = None
  //trigger interval
  @PropertyKey("trigger.interval", false)
  private var _triggerInterval: Option[String] = None

  //wait time in ms for test
  @PropertyKey("test.waittimeMS", false)
  private var _waittimeInMs: Option[Long] = None

  /**
   * Run the file-writer
   *
   * @param ctx - the execution context
   * @param session - the spark-session
   * @return
   */
  override def run(ctx: ExecutionContext)(implicit session: SparkSession): Option[DataFrame] = for {
    fmt <- this._format
    mode <- this._outputMode
    uri <- this._fileUri
    df <- this._view.flatMap(name => ctx.getView(name))
  } yield Try {
    val initWriter = this._options.foldLeft(df.writeStream.format(fmt))((w, o) => w.option(o._1, o._2))
    //with partitionBy
    val partitionWriter = this._partitionBy match {
      case Some(cs) => initWriter.partitionBy(cs.split(","): _*)
      case _ => initWriter
    }
    //combine with trigger
    val triggerWriter = (this._triggerMode, this._triggerInterval) match {
      case (Some(m), Some(t)) if (m == "continuous") => partitionWriter.trigger(Trigger.Continuous(t))
      case (Some(m), Some(t)) if (m == "processingTime") => partitionWriter.trigger(Trigger.ProcessingTime(t))
      case (Some(m), _) if (m == "once") => partitionWriter.trigger(Trigger.Once())
      case _ => partitionWriter
    }
    this._waittimeInMs match {
      case Some(ts) => triggerWriter.outputMode(mode).start(uri).awaitTermination(ts)
      case _ => triggerWriter.outputMode(mode).start(uri).awaitTermination()
    }
  } match {
    case Success(_) => df
    case Failure(ex) => throw new RuntimeException(s"Cannot write data to the target - $uri.", ex)
  }

  /**
   * The trigger mode
   *
   * @param mode
   * @return
   */
  def triggerMode(mode: String): FileStreamWriter = { this._triggerMode = Some(mode); this }

  /**
   * The trigger interval
   * @param duration
   * @return
   */
  def triggerInterval(duration: String): FileStreamWriter = { this._triggerInterval = Some(duration); this }

  /**
   * The output mode
   *
   * @param mode
   * @return
   */
  def outputMode(mode: String): FileStreamWriter = { this._outputMode = Some(mode); this }

  /**
   * Wait time for streaming to execute before shut it down
   *
   * @param waittime
   * @return
   */
  def waitTimeInMs(waittime: Long): FileStreamWriter = { this._waittimeInMs = Some(waittime); this }
}
