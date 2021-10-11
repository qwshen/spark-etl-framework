package com.qwshen.etl.common

import com.qwshen.common.PropertyKey
import com.qwshen.common.io.FileChannel
import com.typesafe.config.Config
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.StructType
import scala.util.{Failure, Success, Try}

/**
 * This FileReadActor is a common base class for FileReader & FileStreamReader
 */
private[etl] abstract class FileReadActor[T] extends Actor { self: T =>
  //the file format
  @PropertyKey("format", true)
  protected var _format: Option[String] = None
  //the options for loading the file
  @PropertyKey("options.*", false)
  protected var _options: Map[String, String] = Map.empty[String, String]

  @PropertyKey("ddlSchemaString", false)
  protected var _ddlSchemaString: Option[String] = None
  @PropertyKey("ddlSchemaFile", false)
  protected var _ddlSchemaFile: Option[String] = None

  //the file location
  @PropertyKey("fileUri", true)
  protected var _fileUri: Option[String] = None

  //the schema of the target data-frame
  protected var _schema: Option[StructType] = None

  /**
   * Initialize the file reader
   *
   * @param config - the configuration object
   * @param session - the spark-session object
   */
  override def init(properties: Seq[(String, String)], config: Config)(implicit session: SparkSession): Unit = {
    super.init(properties, config)

    //prepare schema
    if (this._schema.isEmpty) {
      this._schema = (if (this._ddlSchemaString.nonEmpty) this._ddlSchemaString else this._ddlSchemaFile.map(f => FileChannel.loadAsString(f)))
        .flatMap(ss => Try(StructType.fromDDL(ss)) match {
          case Success(s) => Some(s)
          case Failure(t) => throw new RuntimeException(s"The schema [$ss] is not in valid DDL format.", t)
        })
    }
    validate(this._format, "The format in FileReader is mandatory or its value is invalid.", Seq("csv", "json", "parquet", "avro", "orc"))
    validate(this._fileUri, "The fileUri in FileReader is mandatory.")
  }

  /**
   * The format of the source files.
   *
   * @param fmt
   * @return
   */
  def sourceFormat(fmt: String): T = { this._format = Some(fmt); this }

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
   * The schema of the target data-frame
   *
   * @param schema
   * @return
   */
  def ddlSchema(schema: StructType): T = { this._schema = Some(schema); this }

  /**
   * The source path of the files
   *
   * @param dir
   * @return
   */
  def fileUri(dir: String): T = { this._fileUri = Some(dir); this }
}
