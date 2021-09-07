package com.it21learning.etl.common

import com.it21learning.common.ObjectCreator
import com.it21learning.common.io.FileChannel
import com.it21learning.etl.common.config.KafkaReadConfig
import com.typesafe.config.Config
import io.confluent.kafka.schemaregistry.client.rest.RestService
import org.apache.spark.sql.avro.functions.from_avro
import org.apache.spark.sql.functions.{col, from_json}
import org.apache.spark.sql.types.{DataType, StringType, StructType}
import org.apache.spark.sql.{DataFrame, SparkSession}
import scala.jdk.CollectionConverters._
import scala.util.{Failure, Success, Try}
import scala.xml.NodeSeq

/**
 * The common class for kafka read
 */
private[etl] abstract class KafkaReadActor[T]  extends Actor { self: T =>
  //bootstrap-servers
  protected var _bootStrapServers: Option[String] = None
  //topics
  protected var _topics: Option[String] = None
  //the options for loading the file
  protected var _options: Map[String, String] = Map.empty[String, String]

  //key schema
  protected var _keySchemaType: Option[String] = None
  protected var _keySchema: Option[String] = None
  //the options for loading the key-schema
  protected var _keySchemaOptions: Map[String, String] = Map.empty[String, String]

  //value schema
  protected var _valueSchemaType: Option[String] = None
  protected var _valueSchema: Option[String] = None
  //the options for loading the value-schema
  protected var _valueSchemaOptions: Map[String, String] = Map.empty[String, String]

  //headers
  protected var _includeHeaders: Boolean = false

  private var _config: Option[KafkaReadConfig] = None

  /**
   * Run the file-reader
   *
   * @param ctx - the execution context
   * @param session - the spark-session
   * @return
   */
  def run(ctx: ExecutionContext)(implicit session: SparkSession): Option[DataFrame] = for {
    df <- this.load(session)
  } yield Try {
    //calculate all columns except key & value
    val columns: Seq[String] = df.columns.filter(c => c != "key" && c != "value").map(c => s"$c as __kafka_$c")
    //load the data from kafka
    val dfResult = df
      .withColumn("key", (this._keySchemaType, this._keySchema) match {
        case (Some(t), Some(s)) if (t == "avro") => {
          if (this._keySchemaOptions.nonEmpty)
            from_avro(col("key"), s, this._keySchemaOptions.asJava)
          else
            from_avro(col("key"), s)
        }
        case (Some(t), Some(s)) if (t == "json") => {
          if (this._keySchemaOptions.nonEmpty)
            from_json(col("key").cast(StringType), DataType.fromJson(s).asInstanceOf[StructType], this._keySchemaOptions.asJava)
          else
            from_json(col("key").cast(StringType), DataType.fromJson(s).asInstanceOf[StructType])
        }
        case _ => col("key")
      })
      .withColumn("value", (this._valueSchemaType, this._valueSchema) match {
        case (Some(t), Some(s)) if (t == "avro") => {
          if( this._valueSchemaOptions.nonEmpty)
            from_avro(col("value"), s, this._valueSchemaOptions.asJava)
          else
            from_avro(col("value"), s)
        }
        case (Some(t), Some(s)) if (t == "json") => {
          if( this._valueSchemaOptions.nonEmpty)
            from_json(col("value").cast(StringType), DataType.fromJson(s).asInstanceOf[StructType], this._valueSchemaOptions.asJava)
          else
            from_json(col("value").cast(StringType), DataType.fromJson(s).asInstanceOf[StructType])
        }
        case _ => col("value")
      })

    //determine the final columns
    val fields: Seq[String] = ((dfResult.schema("key").dataType, dfResult.schema("value").dataType) match {
      case (_: StructType, _: StructType) => Seq("key.*", "value.*")
      case (_, _: StructType) => Seq("key", "value.*")
      case (_: StructType, _) => Seq("key.*", "value")
      case _ => Nil
    }) ++ columns
    //apply any post-process
    postLoad(if (fields.nonEmpty) dfResult.selectExpr(fields: _*) else dfResult)
  } match {
    case Success(dfFinal) => dfFinal
    case Failure(ex) => throw new RuntimeException(s"Cannot read data from the source - ${this._topics.getOrElse("")}.", ex)
  }

  //To trigger the initial load
  protected def load(implicit session: SparkSession): Option[DataFrame]
  //The post load process if any
  protected def postLoad(df: DataFrame): DataFrame = df

  /**
   * Initialize the file reader from the xml definition
   *
   * @param config     - the configuration object
   * @param session    - the spark-session object
   */
  override def init(definition: NodeSeq, config: Config)(implicit session: SparkSession): Unit = {
    val properties: Map[String, String] = this.parse(definition, Nil)
    //this._config = ObjectCreator.create[KafkaReadConfig](properties)

    //scan bootstrap-servers & topics first
    (definition \ "property").foreach(prop => (prop \ "@name").text match {
      case "bootstrapServers" => this._bootStrapServers = Some(prop.text)
      case "topics" => this._topics = Some(prop.text)
      case "options" => {
        this._options = (prop \ "option").map(o => ((o \ "@name").text, (o \ "@value").text)).toMap[String, String]
        this._includeHeaders = this._options.exists { case (k, v) => k == "includeHeaders" && v == "true" }
      }
      case _ =>
    })
    validate(this._bootStrapServers, "The bootstrap-servers is mandatory in KafkaReader/KafkaStreamReader.")
    validate(this._topics, "The topics in KafkaReader/KafkaStreamReader is mandatory.,")

    //    val restService = new RestService(schemaRegistryURL)
    //    val props = Map(
    //      "basic.auth.credentials.source" -> "USER_INFO",
    //      "schema.registry.basic.auth.user.info" -> "secret:secret"
    //    ).asJava
    //    var schemaRegistryClient = new CachedSchemaRegistryClient(restService, 100, props)

    //scan key, value
    for {
      topic <- this._topics.map(s => s.split(",").head)
      prop <- (definition \ "property")
      if ((prop \ "@name").text == "keySchema" || (prop \ "@name").text == "valueSchema")
    } (prop \ "@name").text match {
      case "keySchema" => {
        //parse the schema
        (prop \ "definition").headOption.foreach(kd => (kd \ "@name").text match {
          case "avroSchemaUri" => Try(new RestService(kd.text).getLatestVersion(s"$topic-key").getSchema) match {
            case Success(schema) => {
              this._keySchemaType = Some("avro")
              this._keySchema = Some(schema)
            }
            case _ => throw new RuntimeException(s"Cannot extract the key schema for $topic from ${kd.text}.")
          }
          case "avroSchemaString" => {
            this._keySchemaType = Some("avro")
            this._keySchema = Some(kd.text)
          }
          case "avroSchemaFile" => {
            this._keySchemaType = Some("avro")
            this._keySchema = Some(FileChannel.loadAsString(kd.text))
          }
          case "jsonSchemaString" => {
            this._keySchemaType = Some("json")
            this._keySchema = Some(kd.text)
          }
          case "jsonSchemaFile" => {
            this._keySchemaType = Some("json")
            this._keySchema = Some(FileChannel.loadAsString(kd.text))
          }
          case _ =>
        })
        //parse the options
        this._keySchemaOptions = (prop \ "options" \ "option").map(o => ((o \ "@name").text, (o \ "@value").text)).toMap[String, String]
      }
      case "valueSchema" => {
        //parse the value schema
        (prop \ "definition").headOption.foreach(vd => (vd \ "@name").text match {
          case "avroSchemaUri" => Try(new RestService(vd.text).getLatestVersion(s"$topic-value").getSchema) match {
            case Success(schema) => {
              this._valueSchemaType = Some("avro")
              this._valueSchema = Some(schema)
            }
            case _ => throw new RuntimeException(s"Cannot extract the value schema for $topic from ${vd.text}.")
          }
          case "avroSchemaString" => {
            this._valueSchemaType = Some("avro")
            this._valueSchema = Some(vd.text)
          }
          case "avroSchemaFile" => {
            this._valueSchemaType = Some("avro")
            this._valueSchema = Some(FileChannel.loadAsString(vd.text))
          }
          case "jsonSchemaString" => {
            this._valueSchemaType = Some("json")
            this._valueSchema = Some(vd.text)
          }
          case "jsonSchemaFile" => {
            this._valueSchemaType = Some("json")
            this._valueSchema = Some(FileChannel.loadAsString(vd.text))
          }
          case _ =>
        })
        //parse the options
        this._valueSchemaOptions = (prop \ "options" \ "option").map(o => ((o \ "@name").text, (o \ "@value").text)).toMap[String, String]
      }
    }
  }

  /**
   * The bootstrap-servers of the target Kafka cluster.
   *
   * @param servers
   * @return
   */
  def bootstrapServers(servers: String): T = { this._bootStrapServers = Some(servers); this }

  /**
   * The name of the topics
   *
   * @param topics
   * @return
   */
  def topics(topics: String): T = { this._topics = Some(topics); this }

  /**
   * The load option
   *
   * @param name
   * @param value
   * @return
   */
  def readOption(name: String, value: String): T = { this._options = this._options + (name -> value); this }

  /**
   * The load options
   *
   * @param opts
   * @return
   */
  def readOptions(opts: Map[String, String]): T = { this._options = this._options ++ opts; this }

  /**
   * The schema of the key
   *
   * @param schema
   * @return
   */
  def keyAvroSchema(schema: String): T = {
    this._keySchemaType = Some("avro")
    this._keySchema = Some(schema)
    this
  }
  /**
   * The schema of the key
   *
   * @param schema
   * @return
   */
  def keyJsonSchema(schema: String): T = {
    this._keySchemaType = Some("json")
    this._keySchema = Some(schema)
    this
  }

  /**
   * The schema of the value
   *
   * @param schema
   * @return
   */
  def valueAvroSchema(schema: String): T = {
    this._valueSchemaType = Some("avro")
    this._valueSchema = Some(schema)
    this
  }
  /**
   * The schema of the value
   *
   * @param schema
   * @return
   */
  def valueJsonSchema(schema: String): T = {
    this._valueSchemaType = Some("json")
    this._valueSchema = Some(schema)
    this
  }

  /**
   * Include the header in the result
   *
   * @param value
   * @return
   */
  def includeHeaders(value: Boolean = false): T = { this._includeHeaders = value; this }
}
