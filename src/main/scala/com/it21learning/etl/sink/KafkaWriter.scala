package com.it21learning.etl.sink

import com.it21learning.common.io.FileChannel
import com.it21learning.etl.common.{Actor, ExecutionContext, KafkaWriteActor}
import com.typesafe.config.Config
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.avro.functions.to_avro
import io.confluent.kafka.schemaregistry.client.rest.RestService
import org.apache.spark.sql.functions._
import org.apache.avro.Schema.Parser

import scala.util.{Failure, Success, Try}
import scala.xml.NodeSeq

/**
 * This writer writes a data-frame to Kafka topics
 *
 * The following is the definition in xml format
 *   <actor type="com.it21learning.etl.sink.KafkaWriter">
 *     <!-- The kafka bootstrap-servers list. This is mandatory -->
 *     <property name="bootstrapServers">localhost:9092</property>
 *     <!-- The topic which messages will be published into. This is mandatory -->
 *     <property name="topic">users</property>
 *
 *     <!--
 *       The definition of the key, Optional. Either of its schema or key-field-mapping could be defined.
 *       If not specified, a sequence-number is auto-generated and used as the key.
 *       Note: if custom format required, the data needs to be wrapped into the key-column or key-field-mapping specified here.
 *     -->
 *     <property name="key">
 *       <!--
 *         One of the following can be specified.
 *         Fields defined in the schema are collected & the data for these fields is wrapped as the key
 *       -->
 *       <definition name="avroSchemaString">{"schema": "{\"type\": \"string\"}"}</definition>
 *       <definition name="avroSchemaUri">http://user-name:password@localhost:8081</definition>
 *       <definition name="avroSchemaFile">schema/users.asvc</definition>
 *       <definition name="field">user_id</definition>
 *     </property>
 *     <!--
 *       The definition of the value, Optional - either the schema or value-field-mapping could be defined.
 *       If not specified, all columns are wrapped into a json doc and used as the value.
 *       Note: if custom format required, the data needs to be wrapped into the value-volumn or value-field-mapping specified here.
 *     -->
 *     <property name="value">
 *       <!--
 *         One of the following can be specified.
 *         Fields defined in the schema are collected and the data of these fields is wrapped as the value
 *       -->
 *       <definition name="avroSchemaString">{"schema": "{\"type\": \"string\"}"}</definition>
 *       <definition name="avroSchemaUri">http://user-name:password@localhost:8081</definition>
 *       <definition name="avroSchemaFile">schema/users.asvc</definition>
 *       <definition name="field">user_id</definition>
 *     </property>
 *     <!-- which field defines the headers with the message written into kafka -->
 *     <property name="headers" field="timestamp" />
 *
 *     <!-- the target view to be written to Kafka. This is mandatory. -->
 *     <property name="view">events</property>
 *   </actor>
 */
final class KafkaWriter extends KafkaWriteActor[KafkaWriter] {
  //write the dataframe
  protected def write(df: DataFrame): Unit = for {
    _ <- validate(this._bootStrapServers, "The bootstrap-servers in KafkaWriter/KafkaStreamWriter is mandatory.")
    servers <- this._bootStrapServers
    _ <- validate(this._topic, "The topic in KafkaWriter/KafkaStreamWriter is mandatory.")
    topic <- this._topic
  } {
    val columns = if (df.columns.contains("headers")) Seq("key", "value", "headers") else Seq("key", "value")
    df.select(columns.head, columns.tail: _*)
      .write.format("kafka").option("kafka.bootstrap.servers", servers).option("topic", topic).save
  }
}
