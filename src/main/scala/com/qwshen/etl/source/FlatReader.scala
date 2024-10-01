package com.qwshen.etl.source

import com.qwshen.common.PropertyKey
import com.qwshen.common.io.FileChannel
import com.qwshen.etl.utils.DataframeHelper._
import com.qwshen.etl.common.{FlatReadActor, JobContext}
import com.qwshen.etl.common.FlatReadActor.{DelimitedField, PositionalField}
import com.typesafe.config.Config
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

import scala.util.{Failure, Success, Try}
import org.apache.spark.sql.functions.{col, count, from_csv, input_file_name, lit, max, min}
import org.apache.spark.sql.types.StructType
import org.apache.spark.storage.StorageLevel

/**
 * To load a delimited, fixed-length or text flat-file.
 */
class FlatReader extends FlatReadActor[FlatReader] {
  final val FLAT_DELIMITED: String = "delimited"
  final val FLAT_FIXED_LENGTH: String = "fixed-length"
  final val FLAT_TEXT: String = "text"

  private final val _clmnFileName: String = "___input_file__"
  private final val _clmnFileCnt: String = "___input_fn_cnt__"

  /**
   * The character to separate multiple URIs from where objects are loaded
   */
  @PropertyKey("multiUriSeparator", false)
  protected var _multiUriSeparator: Option[String] = None

  @PropertyKey("row.noField", false)
  protected var _noField: Option[String] = None
  @PropertyKey("addInputFile", false)
  protected var _addInputFile: Boolean = false

  @PropertyKey("format", false)
  protected var _format: String = FLAT_TEXT
  @PropertyKey("identifier.matchRgPtn", false)
  protected var _idMatchRgPtn: Option[String] = None

  @PropertyKey("fileHeader.identifier.beginNRows", false)
  protected var _fhIdBeginNRows: Option[Int] = None
  @PropertyKey("fileHeader.identifier.matchRgPtn", false)
  protected var _fhIdMatchRgPtn: Option[String] = None
  @PropertyKey("fileHeader.format", false)
  protected var _fhFormat: String = FLAT_TEXT
  @PropertyKey("fileHeader.options", false)
  protected var _fhOptions: Map[String, String] =Map.empty[String, String]
  @PropertyKey("fileHeader.ddlFieldsString", false)
  protected var _fhDdlFieldsString: Option[String] = None
  @PropertyKey("fileHeader.ddlFieldsFile", false)
  protected var _fhDdlFieldsFile: Option[String] = None
  @PropertyKey("fileHeader.output-view.name", false)
  protected var _fhViewName: Option[String] = None
  @PropertyKey("fileHeader.output-view.global", false)
  protected var _fhViewGlobal: Boolean = false

  @PropertyKey("fileTrailer.identifier.endNRows", false)
  protected var _ftIdEndNRows: Option[Int] = None
  @PropertyKey("fileTrailer.identifier.matchRgPtn", false)
  protected var _ftIdMatchRgPtn: Option[String] = None
  @PropertyKey("fileTrailer.format", false)
  protected var _ftFormat: String = FLAT_TEXT
  @PropertyKey("fileTrailer.options", false)
  protected var _ftOptions: Map[String, String] =Map.empty[String, String]
  @PropertyKey("fileTrailer.ddlFieldsString", false)
  protected var _ftDdlFieldsString: Option[String] = None
  @PropertyKey("fileTrailer.ddlFieldsFile", false)
  protected var _ftDdlFieldsFile: Option[String] = None
  @PropertyKey("fileTrailer.output-view.name", false)
  protected var _ftViewName: Option[String] = None
  @PropertyKey("fileTrailer.output-view.global", false)
  protected var _ftViewGlobal: Boolean = false

  @PropertyKey("fallbackRead", false)
  protected var _fallbackRead: Boolean = false
  @PropertyKey("ddlFallbackSchemaString", false)
  protected var _ddlFallbackSchemaString: Option[String] = None
  @PropertyKey("ddlFallbackSchemaFile", false)
  protected var _ddlFallbackSchemaFile: Option[String] = None
  @PropertyKey("fallbackSqlString", false)
  protected var _fallbackSqlString: Option[String] = None
  @PropertyKey("fallbackSqlFile", false)
  protected var _fallbackSqlFile: Option[String] = None

  //fields
  protected var _fields: Option[Either[Seq[PositionalField], Seq[DelimitedField]]] = None

  //the file-header schema from _fhDdlSchemaString or _fhDdlSchemaFile
  protected var _fileheaderSchema: Option[StructType] = None
  //the file-header fields
  protected var _fileheaderFields: Option[Either[Seq[PositionalField], Seq[DelimitedField]]] = None

  //the file-trailer schema from _ftDdlSchemaString or _ftDdlSchemaFile
  protected var _filetrailerSchema: Option[StructType] = None
  //the file-trailer fields
  protected var _filetrailerFields: Option[Either[Seq[PositionalField], Seq[DelimitedField]]] = None

  //the fallback schema from _ddlFallbackSchemaString or _ddlFallbackSchemaFile
  protected var _fallbackSchema: Option[StructType] = None

  /**
   * Initialize the flat reader
   *
   * @param config - the configuration object
   * @param session - the spark-session object
   */
  override def init(properties: Seq[(String, String)], config: Config)(implicit session: SparkSession): Unit = {
    super.init(properties, config)

    //header schema and fields
    val (fhSchema, fhFields) = setup(this._fhFormat, this._fhDdlFieldsString, this._fhDdlFieldsFile)
    this._fileheaderSchema = fhSchema
    this._fileheaderFields = fhFields
    //body schema and fields
    val (schema, fields) = setup(this._format, this._ddlFieldsString, this._ddlFieldsFile)
    this._schema = schema
    this._fields = fields
    //trailer schema and fields
    val (ftSchema, ftFields) = setup(this._ftFormat, this._ftDdlFieldsString, this._ftDdlFieldsFile)
    this._filetrailerSchema = ftSchema
    this._filetrailerFields = ftFields

    //prepare fallback schema
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
    val dfReader = this._options.foldLeft(session.read.format("text"))((r, o) => r.option(o._1, o._2)).schema(this._defaultSchema)
    var dfRaw = this._multiUriSeparator match {
      case Some(separator) => dfReader.load(uri.split(separator): _*)
      case _ => dfReader.load(uri)
    }
    if (this._addInputFile || ctx.metricsRequired)
      dfRaw = dfRaw.withColumn(this._clmnFileName, input_file_name())
    dfRaw = this._noField.map(nf => dfRaw.zipWithIndex(nf)).getOrElse(dfRaw)
    dfRaw.cache()

    //extract header & trailer data-frames
    val (dfBody: DataFrame, dfHeader: Option[DataFrame], dfTrailer: Option[DataFrame]) = split(dfRaw)
    //formalize data-frames
    for (fhViewName <- this._fhViewName) {
      dfHeader.map(dfHead => formalize(dfHead, this._fhFormat, this._fhOptions, this._fileheaderSchema, this._fileheaderFields))
        .foreach(dfHead => this.registerView(dfHead, fhViewName, this._fhViewGlobal))
    }
    for (ftViewName <- this._ftViewName) {
      dfTrailer.map(dfTrail => formalize(dfTrail, this._ftFormat, this._ftOptions, this._filetrailerSchema, this._filetrailerFields))
        .foreach(dfTrail => this.registerView(dfTrail, ftViewName, this._ftViewGlobal))
    }
    formalize(dfBody, this._format, this._options, this._schema, this._fields)
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

  private def setup(format: String, ddlFieldsString: Option[String], ddlFieldsFile: Option[String]): (Option[StructType], Option[Either[Seq[FlatReadActor.PositionalField], Seq[FlatReadActor.DelimitedField]]]) = {
    format match {
      case FLAT_FIXED_LENGTH =>
        val (schema, fields) = FlatReadActor.parsePositionalFields(ddlFieldsString, ddlFieldsFile)
        (schema, Some(Left(fields)))
      case FLAT_DELIMITED =>
        val (schema, fields) = FlatReadActor.parseDelimitedFields(ddlFieldsString, ddlFieldsFile)
        (schema, Some(Right(fields)))
      case _ => (Some(this._defaultSchema), None)
    }
  }

  private def split(df: DataFrame): (DataFrame, Option[DataFrame], Option[DataFrame]) = {
    var (dfBody: DataFrame, dfHeader: Option[DataFrame], dfTrailer: Option[DataFrame]) = (df, Option.empty[DataFrame], Option.empty[DataFrame])
    val hasFileColumn = df.columns.contains(this._clmnFileName)
    if (this._fhIdBeginNRows.nonEmpty || this._ftIdEndNRows.nonEmpty) {
      var dfData = (if (hasFileColumn) dfBody else dfBody.withColumn(this._clmnFileName, input_file_name())).zipWithIndex("___file_seq_no__", Seq(this._clmnFileName))
      val dfFileNo = dfData.groupBy(col(this._clmnFileName)).agg(
        min(col("___file_seq_no__")).as("___min_file_seq_no__"),
        max(col("___file_seq_no__")).as("___max_file_seq_no__")
      )
      dfData = dfData.alias("d").join(
        dfFileNo.alias("fn"), col(s"d.${this._clmnFileName}") === col(s"fn.${this._clmnFileName}"), "inner"
      ).select(
        col(s"d.${this._valueField}"),
        col(s"d.${this._clmnFileName}"),
        col("___file_seq_no__"),
        col("___min_file_seq_no__"),
        col("___max_file_seq_no__")
      )
      var dfTmpBody = dfData
      val dfTmpHeader = this._fhIdBeginNRows.map(nr => {
        dfTmpBody = dfTmpBody.filter(col("___file_seq_no__") >= col("___min_file_seq_no__") + nr)
        dfData.filter(col("___file_seq_no__") < col("___min_file_seq_no__") + nr).select(col(this._valueField), col(this._clmnFileName))
      })
      val dfTmpTrailer = this._ftIdEndNRows.map(nr => {
        dfTmpBody = dfTmpBody.filter(col("___file_seq_no__") <= col("___max_file_seq_no__") - nr)
        dfData.filter(col("___file_seq_no__") > col("___max_file_seq_no__") - nr).select(col(this._valueField), col(this._clmnFileName))
      })
      dfBody = if (hasFileColumn) dfTmpBody.select(col(this._valueField), col(this._clmnFileName)) else dfTmpBody.select(this._valueField)
      dfHeader = if (hasFileColumn) dfTmpHeader else dfTmpHeader.map(h => h.select(this._valueField))
      dfTrailer = if (hasFileColumn) dfTmpTrailer else dfTmpTrailer.map(t => t.select(this._valueField))
    }

    if (this._fhIdMatchRgPtn.nonEmpty || this._ftIdMatchRgPtn.nonEmpty) {
      var dfTmpBody = dfBody
      if (dfHeader.isEmpty) {
        dfHeader = this._fhIdMatchRgPtn.map(rp => {
          dfTmpBody = dfTmpBody.filter(!col(this._valueField).rlike(rp))
          dfBody.filter(col(this._valueField).rlike(rp))
        })
      }
      if (dfTrailer.isEmpty) {
        dfTrailer = this._ftIdMatchRgPtn.map(rp => {
          dfTmpBody = dfTmpBody.filter(!col(this._valueField).rlike(rp))
          dfBody.filter(col(this._valueField).rlike(rp))
        })
      }
      dfBody = dfTmpBody
    }

    dfBody = this._idMatchRgPtn.map(rp => dfBody.filter(col(this._valueField).rlike(rp))).getOrElse(dfBody)
    (dfBody, dfHeader, dfTrailer)
  }

  private def formalize(df: DataFrame, format: String, options: Map[String, String], schema: Option[StructType], fields: Option[Either[Seq[PositionalField], Seq[DelimitedField]]]): DataFrame = {
    val hasFileColumn = df.columns.contains(this._clmnFileName)
    format match {
      case FLAT_DELIMITED => fields match {
        case Some(Right(dfs)) => schema.map(s => {
          val ddlString = (0 to dfs.reduce((x,y) => if (x.index > y.index) x else y).index).map(idx => dfs.find(_.index == idx) match {
            case Some(x) => s"${x.name} ${x.typ}"
            case _ => s"__dummy_${idx}__ string"
          }).reduce((x, y) => s"${x}, ${y}")
          val ddlSchema = StructType.fromDDL(ddlString)
          val dfDelimited = df.alias("m").withColumn("__csv_", from_csv(col("value"), ddlSchema, options))
            .select("m.*", "__csv_.*")
          dfDelimited.select((if (hasFileColumn) col(this._clmnFileName) +: s.fields.map(field => col(field.name)) else s.fields.map(field => col(field.name))): _*)
        }).getOrElse(df)
        case _ => df
      }
      case FLAT_FIXED_LENGTH => fields match {
        case Some(Left(pfs)) =>schema.map(s => {
          val getType = (name: String) => s.fields.find(_.name.equals(name)).head.dataType
          val posColumns = pfs.map(f => col(this._valueField).substr(f.startPos, f.length).as(f.name).cast(getType(f.name)))
          df.select((if (hasFileColumn) col(this._clmnFileName) +: posColumns else posColumns): _*)
        }).getOrElse(df)
        case _ => df
      }
      case _ => df
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
