package com.it21learning.etl.common

import com.it21learning.common.PropertyKey
import org.apache.avro.Schema.Parser
import org.apache.spark.sql.avro.functions.to_avro
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{BinaryType, DataType, StringType, StructType}
import org.apache.spark.sql.{DataFrame, SparkSession}
import scala.collection.JavaConverters._
import scala.util.{Failure, Success, Try}

/**
 * The common behavior for Kafka Write
 * @tparam T
 */
private[etl] abstract class KafkaWriteActor[T] extends KafkaActor[T] { self: T =>
  //key field
  @PropertyKey("keyField", false)
  protected var _keyField: Option[String] = None
  //value field
  @PropertyKey("valueField", false)
  protected var _valueField: Option[String] = None

  //headers
  @PropertyKey("headerField", false)
  protected var _headerField: Option[String] = None
  //the view for writing out
  @PropertyKey("view", true)
  protected var _view: Option[String] = None

  /**
   * Run the file-reader
   */
  def run(ctx: ExecutionContext)(implicit session: SparkSession): Option[DataFrame] = for {
    df <- this._view.flatMap(name => ctx.getView(name))
  } yield Try {
    def getBySchema(schema: Schema, dstField: String)(x: DataFrame): DataFrame = {
      val fields: Seq[String] = schema.sType match {
        case "avro" => new Parser().parse(schema.sValue).getFields.asScala.map(f => f.name)
        case _ => DataType.fromJson(schema.sValue.stripMargin).asInstanceOf[StructType].fields.map(f => f.name)
      }
      x.withColumn(dstField, to_avro(struct(x.columns.filter(c => fields.contains(c)).map(column): _*), schema.sValue))
    }
    def getByField(srcField: String, dstField: String)(x: DataFrame): DataFrame = x.withColumn(dstField, col(srcField))
    def getKeyDefault(x: DataFrame): DataFrame = if (!x.columns.contains("key")) x.withColumn("key", monotonically_increasing_id.cast(StringType)) else x
    def getValueDefault(x: DataFrame): DataFrame = if (!x.columns.contains("value")) x.withColumn("value", to_json(struct(x.columns.map(column): _*))) else x

    //populate key column
    var dfWithKey = this._keyField.map(field => getByField(field, "key")(df))
      .getOrElse(this._keySchema.map(schema => getBySchema(schema, "key")(df)).getOrElse(getKeyDefault(df)))
    //make sure the key column is in the type of String or Binary
    if (dfWithKey.schema.fields.exists(c => c.name.equals("key") && c.dataType != StringType && c.dataType != BinaryType)) {
      dfWithKey = dfWithKey.withColumn("key", col("Key").cast(StringType))
    }
    //populate value column
    val dfWithValue = this._valueField.map(field => getByField(field, "value")(dfWithKey))
      .getOrElse(this._valueSchema.map(schema => getBySchema(schema, "value")(dfWithKey)).getOrElse(getValueDefault(dfWithKey)))

    //headers
    val dfWithHeaders = this._headerField.map(headerField => dfWithValue.withColumn("headers", col(headerField))).getOrElse(dfWithValue)
    //write to kafka
    write(dfWithHeaders)
  } match {
    case Success(_) => df
    case Failure(ex) => throw new RuntimeException(s"Cannot write data to the target - ${this._topic.getOrElse("")}.", ex)
  }

  //write the dataframe
  protected def write(df: DataFrame): Unit

  /**
   * The field of the key
   *
   * @param field - the name of the key field
   * @return
   */
  def keyField(field: String): T = { this._keyField = Some(field); this }

  /**
   * The field of the value
   *
   * @param field - the name of the field
   * @return
   */
  def valueField(field: String): T = { this._valueField = Some(field); this }

  /**
   * The field of the headers
   *
   * @param field
   * @return
   */
  def headersField(field: String): T = { this._headerField = Some(field); this }

  /**
   * The source view for writing
   *
   * @param view
   * @return
   */
  def sourceView(view: String): T = { this._view = Some(view); this }
}