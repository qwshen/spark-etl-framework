package com.it21learning.etl.sink

import com.it21learning.common.PropertyKey
import com.it21learning.etl.common.KafkaWriteActor
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.streaming.DataStreamWriter
import org.apache.spark.sql.streaming.Trigger

/**
 * This writer writes a data-frame to Kafka topics
 */
final class KafkaStreamWriter extends KafkaWriteActor[KafkaStreamWriter] {
  //the options for the streaming write to Kafka
  @PropertyKey("options.*", false)
  private var _options: Map[String, String] = Map.empty[String, String]

  //trigger mode
  @PropertyKey("trigger.mode", true)
  private var _triggerMode: Option[String] = None
  //trigger interval
  @PropertyKey("trigger.interval", false)
  private var _triggerInterval: Option[String] = None

  //the output mode
  @PropertyKey("outputMode", true)
  private var _outputMode: Option[String] = None
  //wait time in ms for test
  @PropertyKey("test.waittimeMS", false)
  private var _waittimeInMs: Option[Long] = None

  //write the dataframe
  protected def write(df: DataFrame): Unit = for {
    servers <- this._bootStrapServers
    topic <- this._topic
    outputMode <- this._outputMode
  } {
    val columns = if (df.columns.contains("headers")) Seq("key", "value", "headers") else Seq("key", "value")
    //initial writer
    val initQuery: DataStreamWriter[Row] = df.select(columns.head, columns.tail: _*)
      .writeStream.format("kafka").option("kafka.bootstrap.servers", servers).option("topic", topic)
    //combine with trigger
    val triggerQuery = (this._triggerMode, this._triggerInterval) match {
      case (Some(m), Some(t)) if (m == "continuous") => initQuery.trigger(Trigger.Continuous(t))
      case (Some(m), Some(t)) if (m == "processingTime") => initQuery.trigger(Trigger.ProcessingTime(t))
      case (Some(m), _) if (m == "once") => initQuery.trigger(Trigger.Once())
      case _ => initQuery
    }
    //with options & output-mode
    val streamQuery = this._options.foldLeft(triggerQuery)((w, o) => w.option(o._1, o._2)).outputMode(outputMode).start()
    this._waittimeInMs match {
      case Some(ts) => streamQuery.awaitTermination(ts)
      case _ => streamQuery.awaitTermination()
    }
  }

  /**
   * The load option
   *
   * @param name
   * @param value
   * @return
   */
  def writeOption(name: String, value: String): KafkaStreamWriter = { this._options = this._options + (name -> value); this }

  /**
   * The load options
   *
   * @param opts
   * @return
   */
  def writeOptions(opts: Map[String, String]): KafkaStreamWriter = { this._options = this._options ++ opts; this }

  /**
   * The trigger mode
   *
   * @param mode
   * @return
   */
  def triggerMode(mode: String): KafkaStreamWriter = { this._triggerMode = Some(mode); this }

  /**
   * The trigger interval
   * @param duration
   * @return
   */
  def triggerInterval(duration: String): KafkaStreamWriter = { this._triggerInterval = Some(duration); this }

  /**
   * The output mode
   *
   * @param mode
   * @return
   */
  def outputMode(mode: String): KafkaStreamWriter = { this._outputMode = Some(mode); this }

  /**
   * Wait time for streaming to execute before shut it down
   *
   * @param waittime
   * @return
   */
  def waitTimeInMs(waittime: Long): KafkaStreamWriter = { this._waittimeInMs = Some(waittime); this }
}
