package com.qwshen.etl.source

import com.qwshen.etl.common.{FileReadActor, JobContext}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.functions.{col, count, input_file_name, lit}
import org.apache.spark.storage.StorageLevel

import scala.util.{Failure, Success, Try}
import com.qwshen.common.PropertyKey
import com.qwshen.common.io.FileChannel
import com.typesafe.config.Config
import org.apache.spark.sql.types.StructType

/**
 * This FileReader is for loading json, avro & parquet files
 */
class FileReader extends FileReadActor[FileReader] {
  private final val _clmnFileName: String = "___input_fn__"
  private final val _clmnFileCnt: String = "___input_fn_cnt__"

  /**
   * The character to separate multiple URIs from where objects are loaded
   */
  @PropertyKey("multiUriSeparator", false)
  protected var _multiUriSeparator: Option[String] = None

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
   * Initialize the file reader
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
   * Run the file-reader
   *
   * @param ctx - the execution context
   * @param session - the spark-session
   * @return
   */
  def run(ctx: JobContext)(implicit session: SparkSession): Option[DataFrame] = for {
    fmt <- this._format
    uri <- this._fileUri
  } yield Try {
    val reader = this._schema.foldLeft(this._options.foldLeft(session.read.format(fmt))((s, o) => s.option(o._1, o._2)))((r, s) => r.schema(s))
    val df = this._multiUriSeparator match {
      case Some(separator) => reader.load(uri.split(separator): _*)
      case _ => reader.load(uri)
    }
    if (ctx.metricsRequired) df.withColumn(this._clmnFileName, input_file_name()) else df
  } match {
    case Success(df) => df
    case Failure(ex) => if (this._fallbackRead) {
      val dfEmpty: StructType => DataFrame = (schema: StructType) => session.createDataFrame(session.sparkContext.emptyRDD[Row], schema)
      (this._schema, this._fallbackSchema, this._fallbackSqlString) match {
        case (Some(schema), _, _) => dfEmpty(schema)
        case (_, Some(schema), _) => dfEmpty(schema)
        case (_, _, Some(stmt)) => session.sql(stmt)
        case _ => throw new RuntimeException(this._multiUriSeparator.foldLeft(s"Cannot load the file into data-frame - ${this._fileUri}")((r, s) => String.format("%s. [with uri-separator - %s].", r, s)), ex)
      }
    } else {
      throw new RuntimeException(this._multiUriSeparator.foldLeft(s"Cannot load the file into data-frame - ${this._fileUri}")((r, s) => String.format("%s. [with uri-separator - %s].", r, s)), ex)
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
   * The URI separator
   *
   * @param separator - the splitting character
   * @return
   */
  def uriSeparator(separator: String): FileReader = { this._multiUriSeparator = Some(separator); this }

  /**
   * Flag to indicate whether or not the fallback read is enabled
   * @param fbRead
   * @return
   */
  def fallbackRead(fbRead: Boolean): FileReader = { this._fallbackRead = fbRead; this }

  /**
   * The fall-back schema of the target data-frame
   *
   * @param schema
   * @return
   */
  def ddlFallbackSchema(schema: StructType): FileReader = { this._fallbackSchema = Some(schema); this }

  /**
   * Provide the sql-statement for fall-back read.
   * @param sqlStmt
   * @return
   */
  def fallbackSqlString(sqlStmt: String): FileReader = { this._fallbackSqlString = Some(sqlStmt); this }
}
