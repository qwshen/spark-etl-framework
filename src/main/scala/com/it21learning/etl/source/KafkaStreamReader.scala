package com.it21learning.etl.source

import com.it21learning.etl.common.KafkaReadActor
import com.typesafe.config.Config
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.xml.NodeSeq
import scala.util.Try
import org.apache.spark.sql.functions.current_timestamp

/**
 * This stream-reader reads real-time data from kafka topics into a data-frame
 *
 * IMPORTANT Note:
 *   All Kafka built-in columns such as timestamp, partition etc. are renamed to __kafka_*, such as __kafka_timestamp etc.
 *
 * The following is the definition in xml format
 *   <actor type="com.it21learning.etl.source.KafkaStreamReader">
 *     <!-- the kafka bootstrap-servers list. This is mandatory -->
 *     <property name="bootstrapServers">localhost:9092</property>
 *     <!--
 *       The topics from which messages will be read. Multiple topics can be specified with comma(,) as the delimiter.
 *       Patterns such as user* is supported.
 *       This is mandatory.
 *     -->
 *     <property name="topics">users</property>
 *     <!-- the following options are optional. -->
 *
 *     <property name="options">
 *       <!--
 *         The offsets defines the start-point when reading from topics. It could be one of the following values
 *           - latest (-1 in json) - read from the latest offsets.
 *           - earliest (-2 in json) - read from the very beginning.
 *           - {"topic-a": {"0":23, "1": -2}}, {"topic-b": {"0":-1, "1":-2, "3":100}}
 *         Note: This only takes effect when there is no offsets-commit for the current reader.
 *       -->
 *       <option name="startingOffsets|endingOffsets" value="latest" />
 *       <!-- the kafka group id to use in Kafka consumer while reading from kafka -->
 *       <option name="kafka.group.id" value="test" />
 *       <!-- retrieve the header of messages in the kafka topic -->
 *       <option name="includeHeaders" value="true" />
 *       <!--
 *         Whether or not to fail the query when it's possible that data is lost (such as topics are deleted, or offsets are
 *         out of range). The default value is true.
 *       -->
 *       <option name="failOnDataLoss" value="true" />
 *       <!--
 *         The timeout in milliseconds to poll data from Kafka in executors. The default value is 512 milliseconds.
 *       -->
 *       <option name="kafkaConsumer.pollTimeoutMs" value="1024" />
 *       <!--
 *         Number of times to retry before giving up fetching kafka latest offsets. The default value is 3.
 *       -->
 *       <option name="fetchOffset.numRetries" value="3" />
 *       <!--
 *         Milliseconds to wait before retrying to fetch kafka offsets. The default value is 10 milliseconds.
 *       -->
 *       <option name="fetchOffset.retryIntervalMs", value="16" />
 *       <!--
 *         Rate limit on maximum number of offsets processed per trigger interval.
 *         The specified total number of offsets will be proportionally split across topicPartitions of different volume.
 *         The default is none.
 *       -->
 *       <option name="maxOffsetsPerTrigger" value="none" />
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
 *       <options>
 *         <option name="timestampFormat" value="yyyy-MM-dd'T'HH:mm:ss.sss'Z'" />
 *       </options>
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
 *
 *     <!-- Defines the tolerance of how late data can still be processed. The time-typed field must be defined. -->
 *     <property> name="waterMark">
 *       <definition name="timeField">event_time</definition>
 *       <definition name="delayThreshold">10 seconds</definition>
 *     </property>
 *     <!-- to add a customer timestamp in the name of __timestamp -->
 *     <property name="addTimestamp">false</property>
 *   </actor>
 */
final class KafkaStreamReader extends KafkaReadActor[KafkaStreamReader] {
  //add timestamp
  private var _addTimestamp: Boolean = false

  //water-mark time field
  private var _wmTimeField: Option[String] = None
  //water-mark delay duration
  private var _wmDelayThreshold: Option[String] = None

  /**
   * Initialize the file reader from the xml definition
   *
   * @param config     - the configuration object
   * @param session    - the spark-session object
   */
  override def init(definition: NodeSeq, config: Config)(implicit session: SparkSession): Unit = {
    super.init(definition, config)

    //parse for the water-mark information
    for {
      prop <- (definition \ "property")
      if ((prop \ "@name").text == "waterMark" || (prop \ "@name").text == "addTimestamp")
    } (prop \ "@name").text match {
      case "waterMark" => (prop \ "definition").foreach(kd => (kd \ "@name").text match {
        case "timeField" => this._wmTimeField = Some(kd.text)
        case "delayThreshold" => this._wmDelayThreshold = Some(kd.text)
        case _ =>
      })
      case "addTimestamp" => this._addTimestamp = Try(prop.text.toBoolean).getOrElse(false)
    }
  }

  //load data from kafka
  protected def load(implicit session: SparkSession): Option[DataFrame] = for {
    _ <- validate(this._bootStrapServers, "The bootstrap-servers is mandatory in KafkaReader/KafkaStreamReader.")
    servers <- this._bootStrapServers
    _ <- validate(this._topics, "The topics in KafkaReader/KafkaStreamReader is mandatory.,")
    topics <- this._topics
  } yield {
    //topics come as pattern
    val subscribeMode = "\\*".r.findFirstIn(topics) match {
      case Some(_) => "subscribePattern"
      case _ => "subscribe"
    }
    //load the data from kafka
    this._options.foldLeft(session.readStream.format("kafka"))((r, o) => r.option(o._1, o._2))
      .option("kafka.bootstrap.servers", servers).option(subscribeMode, topics).load()
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
   *
   * @param field
   * @return
   */
  def watermarkTimeField(field: String): KafkaStreamReader = { this._wmTimeField = Some(field); this }

  /**
   * Specify teh water-mark delay threshold
   *
   * @param duration
   * @return
   */
  def watermarkDelayThreshold(duration: String): KafkaStreamReader = { this._wmDelayThreshold = Some(duration); this }

  /**
   * Flag of whether or not to add __timestamp with current timestamp.
   *
   * @param value
   * @return
   */
  def addTimestamp(value: Boolean = false): KafkaStreamReader = { this._addTimestamp = value; this }
}
