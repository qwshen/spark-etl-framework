package com.it21learning.etl.source

import com.it21learning.common.PropertyKey
import com.it21learning.common.io.FileChannel
import com.it21learning.etl.common.{Actor, ExecutionContext}
import com.typesafe.config.Config
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.types.StructType
import scala.util.{Failure, Success, Try}
import scala.xml.NodeSeq

/**
 * This FileReader is for loading json, avro & parquet files
 */
final class FileReader() extends Actor {
  //the file format
  @PropertyKey("format", true)
  private var _format: Option[String] = None
  //the options for loading the file
  @PropertyKey("options.*", false)
  private var _options: Map[String, String] = Map.empty[String, String]

  @PropertyKey("ddlSchemaString", false)
  private var _ddlSchemaString: Option[String] = None
  @PropertyKey("ddlSchemaFile", false)
  private var _ddlSchemaFile: Option[String] = None

  //the file location
  @PropertyKey("fileUri", true)
  private var _fileUri: Option[String] = None

  //the schema of the target data-frame
  private var _schema: Option[StructType] = None

  /**
   * Run the file-reader
   *
   * @param ctx - the execution context
   * @param session - the spark-session
   * @return
   */
  def run(ctx: ExecutionContext)(implicit session: SparkSession): Option[DataFrame] = for {
    fmt <- this._format
    uri <- this._fileUri
  } yield Try {
    this._schema.foldLeft(this._options.foldLeft(session.read.format(fmt))((s, o) => s.option(o._1, o._2)))((r, s) => r.schema(s)).load(uri)
  } match {
    case Success(df) => df
    case Failure(ex) => throw new RuntimeException(s"Cannot load the file into data-frame - ${this._fileUri}.", ex)
  }

  /**
   * Initialize the file reader
   *
   * @param config - the configuration object
   * @param session - the spark-session object
   */
  override def init(properties: Seq[(String, String)], config: Config)(implicit session: SparkSession): Unit = {
    super.init(properties, config)

    //prepare schema
    this._schema = (if (this._ddlSchemaString.nonEmpty) this._ddlSchemaString else this._ddlSchemaFile.map(f => FileChannel.loadAsString(f)))
      .flatMap(ss => Try(StructType.fromDDL(ss)) match {
        case Success(s) => Some(s)
        case Failure(t) => throw new RuntimeException(s"The schema [$ss] is not in valid DDL format.", t)
      })
    validate(this._format, "The format in FileReader is mandatory or its value is invalid.", Seq("csv", "json", "parquet", "avro", "orc"))
    validate(this._fileUri, "The fileUri in FileReader is mandatory.")
  }

  /**
   * The format of the source files.
   *
   * @param fmt
   * @return
   */
  def sourceFormat(fmt: String): FileReader = { this._format = Some(fmt); this }

  /**
   * The load option
   *
   * @param name
   * @param value
   * @return
   */
  def readOption(name: String, value: String): FileReader = { this._options = this._options + (name -> value); this }

  /**
   * The load options
   *
   * @param opts
   * @return
   */
  def readOptions(opts: Map[String, String]): FileReader = { this._options = this._options ++ opts; this }

  /**
   * The schema of the target data-frame
   *
   * @param schema
   * @return
   */
  def ddlSchema(schema: StructType): FileReader = { this._schema = Some(schema); this }

  /**
   * The source path of the files
   *
   * @param dir
   * @return
   */
  def sourcePath(dir: String): FileReader = { this._fileUri = Some(dir); this }
}
