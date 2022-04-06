package com.qwshen.etl.source

import com.qwshen.common.PropertyKey
import com.qwshen.etl.common.KafkaReadActor
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.current_timestamp

/**
 * This stream-reader reads real-time data from kafka topics into a data-frame
 */
class KafkaStreamReader extends KafkaReadActor[KafkaStreamReader] {
  //add timestamp
  @PropertyKey("addTimestamp", false)
  protected var _addTimestamp: Boolean = false

  //water-mark time field
  @PropertyKey("watermark.timeField", false)
  protected var _wmTimeField: Option[String] = None
  //water-mark delay duration
  @PropertyKey("watermark.delayThreshold", false)
  protected var _wmDelayThreshold: Option[String] = None

  //load data from kafka
  protected def load(implicit session: SparkSession): Option[DataFrame] = for {
    servers <- this._bootStrapServers
    topic <- this._topic
  } yield {
    //load the data from kafka
    this._options.foldLeft(session.readStream.format("kafka"))((r, o) => r.option(o._1, o._2))
      .option("kafka.bootstrap.servers", servers).option("subscribe", topic).load()
  }

  //The post load process if any
  override protected def postLoad(df: DataFrame): DataFrame = {
    //plug in the special __timestamp with current-timestamp
    val dfResult = if (this._addTimestamp) df.withColumn("__timestamp", current_timestamp) else df
    //enable water-mark if required
    (this._wmTimeField, this._wmDelayThreshold) match {
      case (Some(m), Some(t)) => dfResult.withWatermark(m, t)
      case _ => dfResult
    }
  }

  /**
   * Specify the water-mark time field
   */
  def watermarkTimeField(field: String): KafkaStreamReader = { this._wmTimeField = Some(field); this }

  /**
   * Specify teh water-mark delay threshold
   */
  def watermarkDelayThreshold(duration: String): KafkaStreamReader = { this._wmDelayThreshold = Some(duration); this }

  /**
   * Flag of whether or not to add __timestamp with current timestamp.
   */
  def addTimestamp(value: Boolean = false): KafkaStreamReader = { this._addTimestamp = value; this }
}
