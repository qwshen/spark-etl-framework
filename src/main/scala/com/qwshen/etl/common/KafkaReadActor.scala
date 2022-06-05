package com.qwshen.etl.common

import com.qwshen.common.PropertyKey
import org.apache.spark.sql.functions.{col, from_json}
import org.apache.spark.sql.types.{DataType, StringType, StructType}
import org.apache.spark.sql.{DataFrame, SparkSession}
import scala.jdk.CollectionConverters._
import scala.util.{Failure, Success, Try}

/**
 * The common class for kafka read
 */
private[etl] abstract class KafkaReadActor[T]  extends KafkaActor[T] { self: T =>
  //the options for loading the file
  @PropertyKey("options.*", false)
  protected var _options: Map[String, String] = Map.empty[String, String]

  //the options for loading the key-schema
  @PropertyKey("keySchema.options.*", false)
  protected var _keySchemaOptions: Map[String, String] = Map.empty[String, String]
  //the options for loading the value-schema
  @PropertyKey("valueSchema.options.*", false)
  protected var _valueSchemaOptions: Map[String, String] = Map.empty[String, String]

  /**
   * Run the file-reader
   *
   * @param ctx - the execution context
   * @param session - the spark-session
   * @return
   */
  def run(ctx: JobContext)(implicit session: SparkSession): Option[DataFrame] = for {
    df <- this.load(session)
  } yield Try {
    //calculate all columns except key & value
    val columns: Seq[String] = df.columns.filter(c => c != "key" && c != "value").map(c => s"$c as __kafka_$c")
    //load the data from kafka
    val dfResult = df
      .withColumn("key", this._keySchema match {
        case Some(s) if (s.sType == "avro" || s.sType == "Json") => s.sType match {
          case "avro" =>
            //For Spark 3.*
            import org.apache.spark.sql.avro.functions.from_avro
            if (this._keySchemaOptions.nonEmpty)
              from_avro(col("key"), s.sValue, this._keySchemaOptions.asJava)
            else
              from_avro(col("key"), s.sValue)
            //For Spark 2.*
            //import org.apache.spark.sql.avro.from_avro
            //from_avro(col("key"), s.sValue)
          case _ =>
            if (this._keySchemaOptions.nonEmpty)
              from_json(col("key").cast(StringType), DataType.fromJson(s.sValue).asInstanceOf[StructType], this._keySchemaOptions.asJava)
            else
              from_json(col("key").cast(StringType), DataType.fromJson(s.sValue).asInstanceOf[StructType])
        }
        case _ => col("key")
      })
      .withColumn("value", this._valueSchema match {
        case Some(s) if (s.sType == "avro" || s.sType == "Json") => s.sType match {
          case "avro" =>
            //For Spark 3.*
            import org.apache.spark.sql.avro.functions.from_avro
            if (this._valueSchemaOptions.nonEmpty)
              from_avro(col("value"), s.sValue, this._valueSchemaOptions.asJava)
            else
              from_avro(col("value"), s.sValue)
            //For Spark 2.*
            //import org.apache.spark.sql.avro.from_avro
            //from_avro(col("value"), s.sValue)
          case _ =>
            if (this._valueSchemaOptions.nonEmpty)
              from_json(col("value").cast(StringType), DataType.fromJson(s.sValue).asInstanceOf[StructType], this._valueSchemaOptions.asJava)
            else
              from_json(col("value").cast(StringType), DataType.fromJson(s.sValue).asInstanceOf[StructType])
        }
        case _ => col("value")
      })

    //determine the final columns
    val fields: Seq[String] = ((dfResult.schema("key").dataType, dfResult.schema("value").dataType) match {
      case (_: StructType, _: StructType) => Seq("key.*", "value.*")
      case (_, _: StructType) => Seq("key", "value.*")
      case (_: StructType, _) => Seq("key.*", "value")
      case _ => Seq("key", "value")
    }) ++ columns
    //apply any post-process
    postLoad(if (fields.nonEmpty) dfResult.selectExpr(fields: _*) else dfResult)
  } match {
    case Success(dfFinal) => dfFinal
    case Failure(ex) => throw new RuntimeException(s"Cannot read data from the source - ${this._topic.getOrElse("")}.", ex)
  }

  //To trigger the initial load
  protected def load(implicit session: SparkSession): Option[DataFrame]
  //The post-load process if any
  protected def postLoad(df: DataFrame): DataFrame = df

  /**
   * The load option
   */
  def readOption(name: String, value: String): T = { this._options = this._options + (name -> value); this }

  /**
   * The load options
   */
  def readOptions(opts: Map[String, String]): T = { this._options = this._options ++ opts; this }
}
