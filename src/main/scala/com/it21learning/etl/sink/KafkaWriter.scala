package com.it21learning.etl.sink

import com.it21learning.etl.common.KafkaWriteActor
import org.apache.spark.sql.DataFrame

/**
 * This writer writes a data-frame to Kafka topics
 */
final class KafkaWriter extends KafkaWriteActor[KafkaWriter] {
  //write the dataframe
  protected def write(df: DataFrame): Unit = for {
    servers <- this._bootStrapServers
    topic <- this._topic
  } {
    val columns = if (df.columns.contains("headers")) Seq("key", "value", "headers") else Seq("key", "value")
    df.select(columns.head, columns.tail: _*)
      .write.format("kafka").option("kafka.bootstrap.servers", servers).option("topic", topic).save
  }
}
