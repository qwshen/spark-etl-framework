package com.it21learning.etl.common

import com.it21learning.common.PropertyKey
import com.it21learning.common.io.FileChannel
import org.apache.spark.sql.SparkSession
import io.confluent.kafka.schemaregistry.client.rest.RestService
import com.typesafe.config.Config

private[etl] abstract class KafkaActor[T] extends Actor { self: T =>
  //the kafka schema
  protected case class Schema(sType: String, sValue: String)

  //bootstrap-servers
  @PropertyKey("bootstrapServers", true)
  protected var _bootStrapServers: Option[String] = None
  //topics
  @PropertyKey("topic", true)
  protected var _topic: Option[String] = None

  //key schema
  @PropertyKey("keySchema.avroSchemaString", false)
  protected var _keyAvroSchemaString: Option[String] = None
  @PropertyKey("keySchema.avroSchemaUri", false)
  protected var _keyAvroSchemaUri: Option[String] = None
  @PropertyKey("keySchema.avroSchemaFile", false)
  protected var _keyAvroSchemaFile: Option[String] = None
  @PropertyKey("keySchema.jsonSchemaString", false)
  protected var _keyJsonSchemaString: Option[String] = None
  @PropertyKey("keySchema.jsonSchemaFile", false)
  protected var _keyJsonSchemaFile: Option[String] = None
  //key schema type
  protected var _keySchema: Option[Schema] = None

  //value schema
  @PropertyKey("valueSchema.avroSchemaString", false)
  protected var _valueAvroSchemaString: Option[String] = None
  @PropertyKey("valueSchema.avroSchemaUri", false)
  protected var _valueAvroSchemaUri: Option[String] = None
  @PropertyKey("valueSchema.avroSchemaFile", false)
  protected var _valueAvroSchemaFile: Option[String] = None
  @PropertyKey("valueSchema.jsonSchemaString", false)
  protected var _valueJsonSchemaString: Option[String] = None
  @PropertyKey("valueSchema.jsonSchemaFile", false)
  protected var _valueJsonSchemaFile: Option[String] = None
  //value schema type
  protected var _valueSchema: Option[Schema] = None

  /**
   * Initialize the kafka reader
   */
  override def init(properties: Seq[(String, String)], config: Config)(implicit session: SparkSession): Unit = {
    super.init(properties, config)

    validate(this._bootStrapServers, "The bootstrap-servers is mandatory in KafkaReader/KafkaStreamReader.")
    validate(this._topic, "The topic in KafkaReader/KafkaStreamReader is mandatory.,")

    //the final key-schema
    this._keySchema = calculateSchema(this._keyAvroSchemaString, this._keyAvroSchemaUri, this._keyAvroSchemaFile, this._keyJsonSchemaString, this._keyJsonSchemaFile, "key")
    //the final value-schema
    this._valueSchema = calculateSchema(this._valueAvroSchemaString, this._valueAvroSchemaUri, this._valueAvroSchemaFile, this._valueJsonSchemaString, this._valueJsonSchemaFile, "value")
  }

  /**
   * To calculate the schema based on various schema options. If more than one options are provided, it picks up the schema per the following order:
   *   avroSchemaString
   *   avroSchemaUri
   *   avroSchemaFile
   *   jsonSchemaString
   *   jsonSchemaFile
   */
  private def calculateSchema(avroSchemaString: Option[String], avroSchemaUri: Option[String], avroSchemaFile: Option[String],
                              jsonSchemaString: Option[String], jsonSchemaFile: Option[String], id: String): Option[Schema] = this._topic.map(topic => {
    avroSchemaString.map(s => Schema("avro", s))
      .getOrElse {
        avroSchemaUri.map(uri => Schema("avro", fetchSchema(uri, s"$topic-$id")))
          .getOrElse {
            avroSchemaFile.map(file => Schema("avro", FileChannel.loadAsString(file)))
              .getOrElse {
                jsonSchemaString.map(s => Schema("json", s))
                  .getOrElse(jsonSchemaFile.map(file => Schema("json", FileChannel.loadAsString(file))).getOrElse(Schema("unknown", "")))
              }
          }
      }
  })

  /**
   * Call registry-service to retrieve the schema
   * @param url - the schema url
   * @param schemaId - identify the schema. It is in the format of topic-key / topic-value.
   * @return - the schema string
   *
   * TODO: in future it needs to enhance the service call to have the authentication added
   */
  private def fetchSchema(url: String, schemaId: String): String = {
    new RestService(url).getLatestVersion(schemaId).getSchema
  }

  /**
   * The bootstrap-servers of the target Kafka cluster.
   */
  def bootstrapServers(servers: String): T = { this._bootStrapServers = Some(servers); this }

  /**
   * The name of the topics
   */
  def topics(topic: String): T = { this._topic = Some(topic); this }

  /**
   * The schema of the key
   */
  def keyAvroSchemaString(schema: String): T = { this._keyAvroSchemaString = Some(schema); this }
  /**
   * The schema of the key
   */
  def keyAvroSchemaUri(uri: String): T = { this._keyAvroSchemaUri = Some(uri); this }
  /**
   * The schema of the key
   */
  def keyAvroSchemaFile(file: String): T = { this._keyAvroSchemaFile = Some(file); this }
  /**
   * The schema of the key
   */
  def keyJsonSchemaString(schema: String): T = { this._keyJsonSchemaString = Some(schema); this }
  /**
   * The schema of the key
   */
  def keyJsonSchemaFile(file: String): T = { this._keyJsonSchemaFile = Some(file); this }

  /**
   * The schema of the value
   */
  def valueAvroSchemaString(schema: String): T = { this._valueAvroSchemaString = Some(schema); this }
  /**
   * The schema of the value
   */
  def valueAvroSchemaUri(uri: String): T = { this._valueAvroSchemaUri = Some(uri); this }
  /**
   * The schema of the value
   */
  def valueAvroSchemaFile(file: String): T = { this._valueAvroSchemaFile = Some(file); this }
  /**
   * The schema of the value
   */
  def valueJsonSchemaString(schema: String): T = { this._valueJsonSchemaString = Some(schema); this }
  /**
   * The schema of the value
   */
  def valueJsonSchemaFile(file: String): T = { this._valueJsonSchemaFile = Some(file); this }
}
