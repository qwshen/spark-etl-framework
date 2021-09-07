package com.it21learning.etl.sink

import com.it21learning.etl.common.KafkaWriteActor
import com.typesafe.config.Config
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.streaming.DataStreamWriter
import org.apache.spark.sql.streaming.Trigger
import scala.xml.NodeSeq
import scala.util.Try

/**
 * This writer writes a data-frame to Kafka topics
 *
 * The following is the definition in xml format
 *   <actor type="com.it21learning.etl.sink.KafkaStreamWriter">
 *     <!-- the kafka bootstrap-servers list. This is mandatory -->
 *     <property name="bootstrapServers">localhost:9092</property>
 *     <!-- The topic which messages will be published into. This is mandatory -->
 *     <property name="topic">users</property>
 *
 *     <!-- The options for writing to Kafka -->
 *     <property name="options">
 *       <option name="checkpointLocation" value="/tmp/checkpoint-staging" />
 *     </property>
 *     <!-- The trigger defines how often to write to Kafka -->
 *     <property name="trigger">
 *       <definition name="mode">continuous|processingTime|once</definition>
 *       <definition name="interval">5 seconds</definition>
 *     </property>

 *     <!-- The output mode defines how to output result - completely, incrementally. This is mandatory -->
 *     <property name="outputMode">complete|append|update</property>
 *     <!-- How long to wait for the streaming to execute before shut it down. This is only for test-debug purpose -->
 *     <property name="waitTimeInMs">16000</property>
 *
 *     <!--
 *       The definition of the key, Optional. Either of its schema or key-field-mapping could be defined.
 *       If not specified, a sequence-number is auto-generated and used as the key.
 *       Note: if custom format required, the data needs to be wrapped into the key-column or key-field-mapping specified here.
 *     -->
 *     <property name="key">
 *       <!-- One of the following can be specified -->
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
 *       <!-- One of the following can be specified -->
 *       <definition name="avroSchemaString">{"schema": "{\"type\": \"string\"}"}</definition>
 *       <definition name="avroSchemaUri">http://user-name:password@localhost:8081</definition>
 *       <definition name="avroSchemaFile">schema/users.asvc</definition>
 *       <definition name="field">user_id</definition>
 *     </property>
 *     <!-- which field defines the headers with the message written into kafka -->
 *     <property name="headers" field="timestamp" />
 *
 *     <!-- the target view to be written to Kafka. This is mandatory -->
 *     <property name="view">events</property>
 *   </actor>
 */
final class KafkaStreamWriter extends KafkaWriteActor[KafkaStreamWriter] {
  //the options for loading the file
  private var _options: Map[String, String] = Map.empty[String, String]

  //trigger mode
  private var _triggerMode: Option[String] = None
  //trigger interval
  private var _triggerInterval: Option[String] = None

  //the output mode
  private var _outputMode: Option[String] = None
  //wait time in ms for test
  private var _waittimeInMs: Option[Long] = None

  /**
   * Initialize the file reader from the xml definition
   *
   * @param config  - the configuration object
   * @param session - the spark-session object
   */
  override def init(definition: NodeSeq, config: Config)(implicit session: SparkSession): Unit = {
    super.init(definition, config)

    //scan key, value
    for {
      prop <- (definition \ "property")
    } (prop \ "@name").text match {
      case "options" => this._options = (prop \ "option").map(o => ((o \ "@name").text, (o \ "@value").text)).toMap[String, String]
      case "trigger" => (prop \ "definition").foreach(vd => (vd \ "@name").text match {
        case "mode" => this._triggerMode = Some(vd.text)
        case "interval" => this._triggerInterval = Some(vd.text)
        case _ =>
      })
      case "outputMode" => this._outputMode = Some(prop.text)
      case "waitTimeInMs" => this._waittimeInMs = Try(prop.text.toLong).toOption
      case _ =>
    }

    validate(this._outputMode, "The outputMode in KafkaStreamWriter is mandatory or its value is invalid.", Seq("complete", "append", "update"))
  }

  //write the dataframe
  protected def write(df: DataFrame): Unit = for {
    _ <- validate(this._bootStrapServers, "The bootstrap-servers in KafkaWriter/KafkaStreamWriter is mandatory.")
    servers <- this._bootStrapServers
    _ <- validate(this._topic, "The topic in KafkaWriter/KafkaStreamWriter is mandatory.")
    topic <- this._topic
    _ <- validate(this._outputMode, "The outputMode in KafkaStreamWriter is mandatory or its value is invalid.", Seq("complete", "append", "update"))
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
    this._waittimeInMs match {
      case Some(ts) => this._options.foldLeft(triggerQuery)((w, o) => w.option(o._1, o._2)).outputMode(outputMode).start().awaitTermination(ts)
      case _ => this._options.foldLeft(triggerQuery)((w, o) => w.option(o._1, o._2)).outputMode(outputMode).start().awaitTermination()
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
