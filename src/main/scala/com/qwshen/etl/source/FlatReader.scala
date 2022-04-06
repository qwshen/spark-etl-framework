package com.qwshen.etl.source

import com.qwshen.common.PropertyKey
import com.qwshen.etl.common.{ExecutionContext, FlatReadActor}
import org.apache.spark.sql.{DataFrame, SparkSession}
import scala.util.{Failure, Success, Try}
import org.apache.spark.sql.functions.{input_file_name}

/**
 * To load a text file.
 *
 * The output dataframe has the following columns:
 *   - row_value: the content of each row
 *   - row_no: the sequence number of each row.
 */
class FlatReader extends FlatReadActor[FlatReader] {
  @PropertyKey("row.noField", false)
  protected var _noField: Option[String] = None

  @PropertyKey("addInputFile", false)
  protected var _addInputFile: Boolean = false

  /**
   * Load the flat-file
   *
   * @param ctx - the execution context
   * @param session - the spark-session
   */
  def run(ctx: ExecutionContext)(implicit session: SparkSession): Option[DataFrame] = for {
    uri <- this._fileUri
  } yield Try {
    import session.implicits._
    if (this._addInputFile) {
      val dfInit = this._options.foldLeft(session.read.format("text"))((r, o) => r.option(o._1, o._2)).load(uri).as[String]
        .withColumn("input_file", input_file_name())

      this._format match {
        case Seq(_, _ @ _*) =>
          val getType = (name: String) => this._schema.flatMap(_.fields.find(_.name.equals(name)).map(x => x.dataType)).getOrElse(
            throw new RuntimeException(s"The data type is unknown for column - $name")
          )
          dfInit.select((this._format.map(f => $"value".substr(f.startPos, f.length).as(f.name).cast(getType(f.name))) :+ $"input_file"): _*)
        case _ =>
          val df = dfInit.rdd.zipWithIndex.map(x => (x._1.getString(0), x._1.getString(1), x._2)).toDF("row_value", "input_file", "row_no")
          (this._noField, this._valueField) match {
            case (Some(no), Some(value)) => df.withColumnRenamed("row_no", no).withColumnRenamed("row_value", value)
            case (Some(no), _) => df.withColumnRenamed("row_no", no)
            case (_, Some(value)) => df.withColumnRenamed("row_value", value)
            case _ => df
          }
      }
    } else {
      val dfInit = this._options.foldLeft(session.read.format("text"))((r, o) => r.option(o._1, o._2)).load(uri).as[String]

      this._format match {
        case Seq(_, _ @ _*) =>
          val getType = (name: String) => this._schema.flatMap(_.fields.find(_.name.equals(name)).map(x => x.dataType)).getOrElse(
            throw new RuntimeException(s"The data type is unknown for column - $name")
          )
          dfInit.select(this._format.map(f => $"value".substr(f.startPos, f.length).as(f.name).cast(getType(f.name))): _*)
        case _ =>
          val df = dfInit.rdd.zipWithIndex.map(x => (x._1, x._2)).toDF("row_value", "row_no")
          (this._noField, this._valueField) match {
            case (Some(no), Some(value)) => df.withColumnRenamed("row_no", no).withColumnRenamed("row_value", value)
            case (Some(no), _) => df.withColumnRenamed("row_no", no)
            case (_, Some(value)) => df.withColumnRenamed("row_value", value)
            case _ => df
          }
      }
    }
  } match {
    case Success(df) => df
    case Failure(ex) => throw new RuntimeException(s"Cannot load the flat file - $uri", ex)
  }

  /**
   * The custom noField name
   * @param field
   * @return
   */
  def noField(field: String): FlatReader = { this._noField = Some(field); this }
}
