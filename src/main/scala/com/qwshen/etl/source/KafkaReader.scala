package com.qwshen.etl.source

import com.qwshen.etl.common.KafkaReadActor
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * This reader reads data from kafka topics into a data-frame.
 */
final class KafkaReader extends KafkaReadActor[KafkaReader] {
  //load data from kafka
  protected def load(implicit session: SparkSession): Option[DataFrame] = for {
    servers <- this._bootStrapServers
    topic <- this._topic
  } yield {
    //load the data from kafka
    this._options.foldLeft(session.read.format("kafka"))((r, o) => r.option(o._1, o._2))
      .option("kafka.bootstrap.servers", servers).option("subscribe", topic).load()
  }
}
