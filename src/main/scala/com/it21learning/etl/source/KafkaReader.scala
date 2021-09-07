package com.it21learning.etl.source

import com.it21learning.etl.common.KafkaReadActor
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * This reader reads data from kafka topics into a data-frame.
 *
 * IMPORTANT Note:
 *   All Kafka built-in columns such as timestamp, partition etc. are renamed to __kafka_*, such as __kafka_timestamp etc.
 *
 * The following is the definition in xml format
 *   <actor type="com.it21learning.etl.sink.KafkaReader">
 *     <!-- the kafka bootstrap-servers list. This is mandatory -->
 *     <property name="bootstrapServers">localhost:9092</property>
 *     <!--
 *       The topics from which messages will be read. Multiple topics can be specified with comma(,) as the delimiter.
 *       Patterns such as user* is supported.
 *       This is mandatory.
 *     -->
 *     <property name="topics">users</property>
 *
 *     <!-- the following options are optional. -->
 *     <property name="options">
 *       <!--
 *         The offsets defines the start-point when reading from topics. It could be one of the following values
 *           - latest (-1 in json) - read from the latest offsets.
 *           - earliest (-2 in json) - read from the very beginning.
 *           - {"topic-a": {"0":23, "1": -2}}, {"topic-b": {"0":-1, "1":-2, "3":100}}
 *         Note: This only takes effect when there is no offsets-commit for the current reader.
 *       -->
 *       <definition name="startingOffsets|endingOffsets">latest</definition>
 *       <!-- the kafka group id to use in Kafka consumer while reading from kafka -->
 *       <definition name="kafka.group.id">test</definition>
 *       <!-- retrieve the header of messages in the kafka topic -->
 *       <definition name="includeHeaders">true</definition>
 *       <!--
 *         Whether or not to fail the query when it's possible that data is lost (such as topics are deleted, or offsets are
 *         out of range). The default value is true.
 *       -->
 *       <definition name="failOnDataLoss">true</definition>
 *       <!--
 *         The timeout in milliseconds to poll data from Kafka in executors. The default value is 512 milliseconds.
 *       -->
 *       <definition name="kafkaConsumer.pollTimeoutMs">1024</definition>
 *       <!--
 *         Number of times to retry before giving up fetching kafka latest offsets. The default value is 3.
 *       -->
 *       <definition name="fetchOffset.numRetries">3</definition>
 *       <!--
 *         Milliseconds to wait before retrying to fetch kafka offsets. The default value is 10 milliseconds.
 *       -->
 *       <definition name="fetchOffset.retryIntervalMs">16</definition>
 *       <!--
 *         Rate limit on maximum number of offsets processed per trigger interval.
 *         The specified total number of offsets will be proportionally split across topicPartitions of different volume.
 *         The default is none.
 *       -->
 *       <definition name="maxOffsetsPerTrigger">none</definition>
 *     </property>
 *
 *     <!--
 *       The definition of the key-schema, Optional.
 *       If not specified, the original key from kafka is populated into the key column of the target data-frame.
 *     -->
 *     <property name="keySchema">
 *       <!-- One of the following can be specified -->
 *       <definition name="avroSchemaString">{"schema": "{\"type\": \"string\"}"}</definition>
 *       <definition name="avroSchemaUri">https://user-name:password@localhost:8081</definition>
 *       <definition name="avroSchemaFile">schema/users-key.asvc</definition>
 *       <definition name="jsonSchemaString">{"type":"struct","fields":[{"name":"user","type":"string","nullable":true},{"name":"event","type":"string","nullable":true}]}</definition>
 *       <definition name="jsonSchemaFile">schema/users-key.jschema</definition>
 *       <!- options used when parsing the avro/json docs. -->
 *       <definition name="options">
 *         <definition name="timestampFormat">"yyyy-MM-dd'T'HH:mm:ss.sss'Z'"</definition>
 *       </definition>
 *     </property>
 *     <!--
 *       The definition of the value-schema, Optional.
 *       If not specified, the original value from kafka is populated into the value column of the target data-frame.
 *     -->
 *     <property name="valueSchema">
 *       <!-- One of the following can be specified -->
 *       <definition name="avroSchemaString">{"schema": "{\"type\": \"string\"}"}</definition>
 *       <definition name="avroSchemaUri">https://user-name:password@localhost:8081</definition>
 *       <definition name="avroSchemaFile">schema/users.asvc</definition>
 *       <definition name="jsonSchemaString">{"type":"struct","fields":[{"name":"user","type":"string","nullable":true},{"name":"event","type":"string","nullable":true}]}</definition>
 *       <definition name="jsonSchemaFile">schema/users.jschema</definition>
 *     </property>
 *   </actor>
 */
final class KafkaReader extends KafkaReadActor[KafkaReader] {
  //load data from kafka
  protected def load(implicit session: SparkSession): Option[DataFrame] = for {
    _ <- validate(this._bootStrapServers, "The bootstrap-servers is mandatory in KafkaReader/KafkaStreamReader.")
    servers <- this._bootStrapServers
    _ <- validate(this._topics, "The topics in KafkaReader/KafkaStreamReader is mandatory.,")
    topics <- this._topics
  } yield {
    //topics come as pattern?
    val subscribeMode = "\\*".r.findFirstIn(topics) match {
      case Some(_) => "subscribePattern"
      case _ => "subscribe"
    }
    //load the data from kafka
    this._options.foldLeft(session.read.format("kafka"))((r, o) => r.option(o._1, o._2))
      .option("kafka.bootstrap.servers", servers).option(subscribeMode, topics).load()
  }
}
