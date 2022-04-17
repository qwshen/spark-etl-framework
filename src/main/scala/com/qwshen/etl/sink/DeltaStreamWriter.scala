package com.qwshen.etl.sink

import com.qwshen.etl.common.DeltaWriteActor
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.{DataFrame, SparkSession}
import com.qwshen.common.PropertyKey
import com.typesafe.config.Config

import scala.util.Try

/**
 * This writer writes a data-frame to delta-lake in streaming mode.
 */
class DeltaStreamWriter extends DeltaWriteActor[DeltaStreamWriter] {
  //output mode
  @PropertyKey("outputMode", true)
  protected var _outputMode: Option[String] = None

  //trigger mode
  @PropertyKey("trigger.mode", true)
  protected var _triggerMode: Option[String] = None
  //trigger interval
  @PropertyKey("trigger.interval", true)
  protected var _triggerInterval: Option[String] = None

  //wait time in ms for test
  @PropertyKey("test.waittimeMS", false)
  protected var _waittimeInMs: Option[Long] = None

  /**
   * Initialize the delta-stream-readers
   */
  override def init(properties: Seq[(String, String)], config: Config)(implicit session: SparkSession): Unit = {
    Try (super.init(properties, config))

    this.validate(this._sinkPath, "The source-path is mandatory for delta stream writers")
    this.validate(this._outputMode, "The output-mode in delta stream writer must be either complete or append.", Seq("complete", "append"))
  }

  //write the dataframe
  protected def write(df: DataFrame): Unit = for {
    path <- this._sinkPath
    mode <- this._outputMode
  } {
    val initQuery = this._options.foldLeft(df.writeStream.format("delta"))((w, o) => w.option(o._1, o._2)).outputMode(mode)

    //with partitionBy
    val partitionQuery = this._partitionBy match {
      case Some(cs) => initQuery.partitionBy(cs.split(","): _*)
      case _ => initQuery
    }

    //combine with trigger
    val triggerQuery = (this._triggerMode, this._triggerInterval) match {
      case (Some(m), Some(t)) if (m == "continuous") => partitionQuery.trigger(Trigger.Continuous(t))
      case (Some(m), Some(t)) if (m == "processingTime") => partitionQuery.trigger(Trigger.ProcessingTime(t))
      case (Some(m), _) if (m == "once") => partitionQuery.trigger(Trigger.Once())
      case _ => partitionQuery
    }

    this._waittimeInMs match {
      case Some(ts) => triggerQuery.start(path).awaitTermination(ts)
      case _ => triggerQuery.start(path).awaitTermination()
    }
  }

  /**
   * Define the output-mode. The valid values are: complete, append, update
   *
   * @param value
   * @return
   */
  def outputMode(value: String): DeltaStreamWriter = { this._outputMode = Some(value); this }

  /**
   * The trigger mode
   *
   * @param mode
   * @return
   */
  def triggerMode(mode: String): DeltaStreamWriter = { this._triggerMode = Some(mode); this }

  /**
   * The trigger interval
   * @param duration
   * @return
   */
  def triggerInterval(duration: String): DeltaStreamWriter = { this._triggerInterval = Some(duration); this }

  /**
   * Wait time for streaming to execute before shut it down
   *
   * @param waittime
   * @return
   */
  def waitTimeInMs(waittime: Long): DeltaStreamWriter = { this._waittimeInMs = Some(waittime); this }
}
