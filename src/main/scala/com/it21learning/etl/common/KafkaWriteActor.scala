package com.it21learning.etl.common

import com.it21learning.common.io.FileChannel
import com.typesafe.config.Config
import io.confluent.kafka.schemaregistry.client.rest.RestService
import org.apache.avro.Schema.Parser
import org.apache.spark.sql.avro.functions.to_avro
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}
import scala.collection.JavaConverters._
import scala.util.{Failure, Success, Try}
import scala.xml.NodeSeq

/**
 * The common behavior for Kafka Write
 * @tparam T
 */
private[etl] abstract class KafkaWriteActor[T] extends Actor { self: T =>
  //bootstrap-servers
  protected var _bootStrapServers: Option[String] = None
  //topic
  protected var _topic: Option[String] = None

  //key schema in avro format
  protected var _keySchema: Option[String] = None
  //key field
  protected var _keyField: Option[String] = None

  //value schema in avro format
  protected var _valueSchema: Option[String] = None
  //value field
  protected var _valueField: Option[String] = None

  //headers
  protected var _headers: Option[String] = None

  //the view for writing out
  protected var _view: Option[String] = None

  /**
   * Run the file-reader
   *
   * @param ctx     - the execution context
   * @param session - the spark-session
   * @return
   */
  def run(ctx: ExecutionContext)(implicit session: SparkSession): Option[DataFrame] = for {
    _ <- validate(this._view, "The view in KafkaWriter/KafkaStreamWriter is mandatory.")
    df <- this._view.flatMap(name => ctx.getView(name))
  } yield Try {
    //populate key column
    val dfWithKey = if (this._keySchema.isDefined) {
      val fields = new Parser().parse(this._keySchema.get).getFields.asScala.map(f => f.name)
      df.withColumn("key", to_avro(struct(df.columns.filter(c => fields.contains(c)).map(column): _*), this._keySchema.get))
    }
    else if (this._keyField.isDefined) {
      df.withColumn("key", col(this._keyField.get))
    }
    else if (!df.columns.contains("key")) {
      import org.apache.spark.sql.types.StringType
      df.withColumn("key", monotonically_increasing_id.cast(StringType))
    }
    else df

    //populate value column
    val dfWithValue = if (this._valueSchema.isDefined) {
      val fields = new Parser().parse(this._valueSchema.get).getFields.asScala.map(f => f.name)
      dfWithKey.withColumn("value", to_avro(struct(df.columns.filter(c => fields.contains(c)).map(column): _*), this._valueSchema.get))
    }
    else if (this._valueField.isDefined) {
      dfWithKey.withColumn("value", col(this._valueField.get))
    }
    else if (!df.columns.contains("value")) {
      dfWithKey.withColumn("value", to_json(struct(df.columns.map(column): _*)))
    }
    else dfWithKey

    //headers
    val dfWithHeaders = if (this._headers.isDefined) {
      dfWithValue.withColumn("headers", col(this._headers.get))
    }
    else dfWithValue

    //write to kafka
    write(dfWithHeaders)
  } match {
    case Success(_) => df
    case Failure(ex) => throw new RuntimeException(s"Cannot write data to the target - ${this._topic.getOrElse("")}.", ex)
  }

  //write the dataframe
  protected def write(df: DataFrame): Unit

  /**
   * Initialize the file reader from the xml definition
   *
   * @param config  - the configuration object
   * @param session - the spark-session object
   */
  override def init(definition: NodeSeq, config: Config)(implicit session: SparkSession): Unit = {
    super.init(config)

    //scan bootstrap-servers & topics first
    (definition \ "property").foreach(prop => (prop \ "@name").text match {
      case "bootstrapServers" => this._bootStrapServers = Some(prop.text)
      case "topic" => this._topic = Some(prop.text)
      case "view" => this._view = Some(prop.text)
      case _ =>
    })

    validate(this._bootStrapServers, "The bootstrap-servers in KafkaWriter/KafkaStreamWriter is mandatory.")
    validate(this._topic, "The topic in KafkaWriter/KafkaStreamWriter is mandatory.")
    validate(this._view, "The view in KafkaWriter/KafkaStreamWriter is mandatory.")

    //    val restService = new RestService(schemaRegistryURL)
    //    val props = Map(
    //      "basic.auth.credentials.source" -> "USER_INFO",
    //      "schema.registry.basic.auth.user.info" -> "secret:secret"
    //    ).asJava
    //    var schemaRegistryClient = new CachedSchemaRegistryClient(restService, 100, props)

    //scan key, value
    for {
      topic <- this._topic
      prop <- (definition \ "property")
      if ((prop \ "@name").text == "key" || (prop \ "@name").text == "value" || (prop \ "@name").text == "headers")
    } (prop \ "@name").text match {
      case "key" => (prop \ "definition").headOption.foreach(kd => (kd \ "@name").text match {
        case "avroSchemaUri" => Try(new RestService(kd.text).getLatestVersion(s"$topic-key").getSchema) match {
          case Success(schema) => this._keySchema = Some(schema)
          case _ => throw new RuntimeException(s"Cannot extract the key schema for $topic from ${kd.text}.")
        }
        case "avroSchemaString" => this._keySchema = Some(kd.text)
        case "avroSchemaFile" => this._keySchema = Some(FileChannel.loadAsString(kd.text))
        case "field" => this._keyField = Some(kd.text)
        case _ =>
      })
      case "value" => (prop \ "definition").headOption.foreach(vd => (vd \ "@name").text match {
        case "avroSchemaUri" => Try(new RestService(vd.text).getLatestVersion(s"$topic-value").getSchema) match {
          case Success(schema) => this._valueSchema = Some(schema)
          case _ => throw new RuntimeException(s"Cannot extract the value schema for $topic from ${vd.text}.")
        }
        case "avroSchemaString" => this._valueSchema = Some(vd.text)
        case "avroSchemaFile" => this._valueSchema = Some(FileChannel.loadAsString(vd.text))
        case "field" => this._valueField = Some(vd.text)
        case _ =>
      })
      case "headers" => this._headers = Some(prop.text)
    }
  }

  /**
   * The bootstrap-servers of the target Kafka cluster.
   *
   * @param servers
   * @return
   */
  def bootstrapServers(servers: String): T = {
    this._bootStrapServers = Some(servers); this
  }

  /**
   * The name of the topics
   *
   * @param topic
   * @return
   */
  def topics(topic: String): T = {
    this._topic = Some(topic); this
  }

  /**
   * The schema of the key
   *
   * @param schema
   * @return
   */
  def keySchema(schema: String): T = {
    this._keySchema = Some(schema)
    this._keyField = None
    this
  }

  /**
   * The field of the key
   *
   * @param field
   * @return
   */
  def keyField(field: String): T = {
    this._keyField = Some(field)
    this._keySchema = None
    this
  }

  /**
   * The schema of the value
   *
   * @param schema
   * @return
   */
  def valueSchema(schema: String): T = {
    this._valueSchema = Some(schema)
    this._valueField = None
    this
  }

  /**
   * The field of the value
   *
   * @param field
   * @return
   */
  def valueField(field: String): T = {
    this._valueField = Some(field)
    this._valueSchema = None
    this
  }

  /**
   * The field of the headers
   *
   * @param field
   * @return
   */
  def headersField(field: String): T = { this._headers = Some(field); this }

  /**
   * The source view for writing
   *
   * @param view
   * @return
   */
  def sourceView(view: String): T = { this._view = Some(view); this }
}