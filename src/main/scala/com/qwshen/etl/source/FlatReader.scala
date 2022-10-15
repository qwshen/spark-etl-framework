package com.qwshen.etl.source

import com.qwshen.common.PropertyKey
import com.qwshen.common.io.FileChannel
import com.qwshen.etl.common.{FlatReadActor, JobContext}
import com.typesafe.config.Config
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import scala.util.{Failure, Success, Try}
import org.apache.spark.sql.functions.{col, count, input_file_name, lit}
import org.apache.spark.sql.types.StructType
import org.apache.spark.storage.StorageLevel

/**
 * To load a text file.
 *
 * The output dataframe has the following columns:
 *   - row_value: the content of each row
 *   - row_no: the sequence number of each row.
 */
class FlatReader extends FlatReadActor[FlatReader] {
  private final val _clmnFileName: String = "___input_file__"
  private final val _clmnFileCnt: String = "___input_fn_cnt__"

  @PropertyKey("row.noField", false)
  protected var _noField: Option[String] = None
  @PropertyKey("addInputFile", false)
  protected var _addInputFile: Boolean = false

  @PropertyKey("fallbackRead", false)
  protected var _fallbackRead: Boolean = false
  //the fallback schema from _ddlFallbackSchemaString or ddlFallbackSchemaFile
  protected var _fallbackSchema: Option[StructType] = None

  @PropertyKey("ddlFallbackSchemaString", false)
  protected var _ddlFallbackSchemaString: Option[String] = None
  @PropertyKey("ddlFallbackSchemaFile", false)
  protected var _ddlFallbackSchemaFile: Option[String] = None
  @PropertyKey("fallbackSqlString", false)
  protected var _fallbackSqlString: Option[String] = None
  @PropertyKey("fallbackSqlFile", false)
  protected var _fallbackSqlFile: Option[String] = None

  /**
   * Initialize the flat reader
   *
   * @param config - the configuration object
   * @param session - the spark-session object
   */
  override def init(properties: Seq[(String, String)], config: Config)(implicit session: SparkSession): Unit = {
    super.init(properties, config)

    //prepare schema
    if (this._fallbackRead) {
      if (this._fallbackSchema.isEmpty) {
        this._fallbackSchema = (if (this._ddlFallbackSchemaString.nonEmpty) this._ddlFallbackSchemaString else this._ddlFallbackSchemaFile.map(f => FileChannel.loadAsString(f)))
          .flatMap(ss => Try(StructType.fromDDL(ss)) match {
            case Success(s) => Some(s)
            case Failure(t) => throw new RuntimeException(s"The fallback-schema [$ss] is not in valid DDL format.", t)
          })
      }
      if (this._fallbackSqlString.isEmpty && this._fallbackSqlFile.isDefined) {
        this._fallbackSqlString = this._fallbackSqlFile.map(f => FileChannel.loadAsString(f))
      }
      validate(Seq(this._schema, this._fallbackSchema, this._fallbackSqlString), "The Schema, or Fallback-Schema or Fallback SQL-String must be provided when fallback-read is enabled.")
    }
  }

  /**
   * Load the flat-file
   *
   * @param ctx - the execution context
   * @param session - the spark-session
   */
  def run(ctx: JobContext)(implicit session: SparkSession): Option[DataFrame] = for {
    uri <- this._fileUri
  } yield Try {
    import session.implicits._
    if (this._addInputFile || ctx.metricsRequired) {
      val dfInit = this._options.foldLeft(session.read.format("text"))((r, o) => r.option(o._1, o._2)).load(uri).as[String]
        .withColumn(this._clmnFileName, input_file_name())

      this._format match {
        case Seq(_, _ @ _*) =>
          val getType = (name: String) => this._schema.flatMap(_.fields.find(_.name.equals(name)).map(x => x.dataType)).getOrElse(
            throw new RuntimeException(s"The data type is unknown for column - $name")
          )
          dfInit.select((this._format.map(f => $"value".substr(f.startPos, f.length).as(f.name).cast(getType(f.name))) :+ col(this._clmnFileName)): _*)
        case _ =>
          val df = dfInit.rdd.zipWithIndex.map(x => (x._1.getString(0), x._1.getString(1), x._2)).toDF("row_value", this._clmnFileName, "row_no")
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

  /**
   * Calculate the rows count of each file
   * @param df
   *  @return
   */
  override def collectMetrics(df: Option[DataFrame]): Seq[(String, String)] = df.map(df => {
    if (!(df.storageLevel.useMemory || df.storageLevel.useDisk || df.storageLevel.useOffHeap)) {
      df.persist(StorageLevel.MEMORY_AND_DISK)
    }

    df.groupBy(col(this._clmnFileName))
      .agg(count(lit(1)).as(this._clmnFileCnt))
    .select(col(this._clmnFileName), col(this._clmnFileCnt)).collect().zipWithIndex
      .map { case(r, i) => ((r(0).toString, String.format("%s", r(1).toString)), i) }
      .flatMap { case(r, i) => Seq((s"input-file${i + 1}-name", r._1), (s"input-file${i + 1}-row-count", r._2)) }.toSeq
  }).getOrElse(Nil)

  /**
   * The custom noField name
   * @param field
   * @return
   */
  def noField(field: String): FlatReader = { this._noField = Some(field); this }

  /**
   * Flag to indicate whether or not the fallback read is enabled
   * @param fbRead
   * @return
   */
  def fallbackRead(fbRead: Boolean): FlatReader = { this._fallbackRead = fbRead; this }

  /**
   * The fall-back schema of the target data-frame
   *
   * @param schema
   * @return
   */
  def ddlFallbackSchema(schema: StructType): FlatReader = { this._fallbackSchema = Some(schema); this }

  /**
   * Provide the sql-statement for fall-back read.
   * @param sqlStmt
   * @return
   */
  def fallbackSqlString(sqlStmt: String): FlatReader = { this._fallbackSqlString = Some(sqlStmt); this }
}
