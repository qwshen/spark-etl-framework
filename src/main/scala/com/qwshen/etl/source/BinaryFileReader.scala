package com.qwshen.etl.source

import com.qwshen.common.PropertyKey
import com.qwshen.etl.common.FlatReadActor.{DelimitedField, PositionalField}
import com.qwshen.etl.common.JobContext
import com.qwshen.etl.functions.BinarySplitter.binary_split
import com.typesafe.config.Config
import org.apache.spark.sql.functions.{col, expr, input_file_name, lit}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.types._
import scala.util.{Failure, Success, Try}

class BinaryFileReader extends FlatFileReader {
  protected val _rawField: String = "__raw_"

  //the length of each record
  @PropertyKey("recordLength", false)
  protected var _recordLength: Option[Int] = None

  @PropertyKey("row.transformation", false)
  protected var _rowTransformation: Option[String] = None

  @PropertyKey("header.fieldTransformation.*", false)
  protected var _fhFieldTransformations: Map[String, String] = Map.empty[String, String]
  @PropertyKey("fieldTransformation.*", false)
  protected var _fieldTransformations: Map[String, String] = Map.empty[String, String]
  @PropertyKey("trailer.fieldTransformation.*", false)
  protected var _ftFieldTransformations: Map[String, String] = Map.empty[String, String]

  /**
   * Initialize the flat reader
   *
   * @param config - the configuration object
   * @param session - the spark-session object
   */
  override def init(properties: Seq[(String, String)], config: Config)(implicit session: SparkSession): Unit = {
    super.init(properties, config)

    this._defaultSchema = StructType(Seq(StructField(this._valueField, BinaryType)))
  }

  /**
   * Run the actor
   *
   * @param ctx     - the execution context
   * @param session - the spark-session
   * @return
   */
  override def run(ctx: JobContext)(implicit session: SparkSession): Option[DataFrame] = for {
    uri <- this._fileUri
  } yield Try {
    var dfRaw = this._multiUriSeparator.map(separator => uri.split(separator)).getOrElse(Array(uri)).map(
      uri_f => {
        val df = this._recordLength match {
          case Some(rl) =>
            val rdd = session.sparkContext.binaryRecords(uri_f, rl).map(r => Row(r))
            session.createDataFrame(rdd, this._defaultSchema)
          case _ => this._options.foldLeft(session.read.format("binaryFile"))((s, o) => s.option(o._1, o._2)).schema(this._defaultSchema).load(uri_f)
        }
        df.select(col(this._valueField), input_file_name().as(this._clmnFileName))
      }
    ).reduce((x, y) => x.union(y))

    //apply row transformation
    dfRaw = this._rowTransformation match {
      case Some(rt) => dfRaw.withColumn(this._rawField, col(this._valueField)).withColumn(this._valueField, expr(rt.replace("$.", this._valueField)))
      case _ => dfRaw
    }
    this.process(dfRaw, ctx)
  } match {
    case Success(df) => df
    case Failure(ex) => if (this._fallbackRead) {
      val dfEmpty: StructType => DataFrame = (schema: StructType) => session.createDataFrame(session.sparkContext.emptyRDD[Row], schema)
      (this._schema, this._fallbackSchema, this._fallbackSqlString) match {
        case (Some(schema), _, _) => dfEmpty(schema)
        case (_, Some(schema), _) => dfEmpty(schema)
        case (_, _, Some(stmt)) => session.sql(stmt)
        case _ => throw new RuntimeException(s"Cannot load the flat file - $uri", ex)
      }
    } else {
      throw new RuntimeException(s"Cannot load the flat file - $uri", ex)
    }
  }

  protected override def formalizeHeader(df: DataFrame, format: String, options: Map[String, String], schema: Option[StructType], fields: Option[Either[Seq[PositionalField], Seq[DelimitedField]]]): DataFrame = {
    formalize(df, format, options, schema, fields, this._fhFieldTransformations)
  }

  protected override def formalizeBody(df: DataFrame, format: String, options: Map[String, String], schema: Option[StructType], fields: Option[Either[Seq[PositionalField], Seq[DelimitedField]]]): DataFrame = {
    formalize(df, format, options, schema, fields, this._fieldTransformations)
  }

  protected override def formalizeTrailer(df: DataFrame, format: String, options: Map[String, String], schema: Option[StructType], fields: Option[Either[Seq[PositionalField], Seq[DelimitedField]]]): DataFrame = {
    formalize(df, format, options, schema, fields, this._ftFieldTransformations)
  }

  protected override def formalize(df: DataFrame, format: String, options: Map[String, String], schema: Option[StructType], fields: Option[Either[Seq[PositionalField], Seq[DelimitedField]]]): DataFrame = {
    if (df.columns.contains(this._rawField)) super.formalize(df, format, options, schema, fields) else {
      val nvColumns = df.columns.map(column => col(column))
      format match {
        case FLAT_DELIMITED => fields match {
          case Some(Right(dfs)) => schema.map(s => {
            val ddlString = (0 to dfs.reduce((x,y) => if (x.index > y.index) x else y).index).map(idx => dfs.find(_.index == idx) match {
              case Some(x) => s"${x.name} ${x.typ}"
              case _ => s"__dummy_${idx}__ string"
            }).reduce((x, y) => s"${x}, ${y}")
            val ddlSchema = StructType.fromDDL(ddlString)
            val delimiter: Array[Byte] = options.get("delimiter").map(s => s.split(",").map(x => java.lang.Integer.decode(x).toByte)).getOrElse(Array[Byte]())
            val dfDelimited = df.withColumn("__binary_", binary_split(col(this._valueField), lit(delimiter)))
              .select(nvColumns ++ ddlSchema.fields.zipWithIndex.map(field => col("__binary_").getItem(field._2).as(field._1.name)): _*)
            dfDelimited.select(nvColumns ++ s.fields.map(field => col(field.name)): _*)
          }).getOrElse(df)
          case _ => df
        }
        case FLAT_FIXED_LENGTH => fields match {
          case Some(Left(pfs)) => schema.map(s => {
            val posColumns = pfs.map(f => col(this._valueField).substr(f.startPos, f.length).as(f.name))
            df.select(nvColumns ++ posColumns: _*)
          }).getOrElse(df)
          case _ => df
        }
        case _ => df
      }
    }
  }

  protected def formalize(df: DataFrame, format: String, options: Map[String, String], schema: Option[StructType], fields: Option[Either[Seq[PositionalField], Seq[DelimitedField]]], fieldTransformations: Map[String, String]): DataFrame = {
    val dfOutput = formalize(df, format, options, schema, fields)

    //apply transformation
    val source = if (dfOutput.columns.contains(this._rawField)) this._rawField else this._valueField
    val getColumn = (name: String) => name match {
      case x if fieldTransformations.contains(x) => expr(fieldTransformations(x).replace("$.", source)).as(x)
      case y if fieldTransformations.contains("default") => expr(fieldTransformations("default").replace("$.", y)).as(y)
      case z => col(name)
    }
    schema.map(s => {
      val getType = (name: String) => s.fields.find(_.name.equals(name)).head.dataType
      dfOutput.select(s.fields.map(f => getColumn(f.name).cast(getType(f.name))): _*)
    }).getOrElse(dfOutput)
      .drop(source)
  }
}
