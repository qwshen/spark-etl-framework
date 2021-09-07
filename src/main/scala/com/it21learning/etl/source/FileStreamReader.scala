package com.it21learning.etl.source

import com.it21learning.common.PropertyKey
import com.it21learning.common.io.FileChannel
import com.it21learning.etl.common.{Actor, ExecutionContext}
import com.typesafe.config.Config
import org.apache.spark.sql.functions.current_timestamp
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, SparkSession}
import scala.util.{Failure, Success, Try}
import scala.xml.NodeSeq

/**
 * This loader is for loading json, avro & parquet files
 *
 * The following is the definition in xml format
 *   <actor type="com.it21learning.etl.source.FileReader">
 *     <!-- the format of the source. This is mandatory -->
 *     <property name="format">csv</property>
 *
 *     <!-- the options for read -->
 *     <property name="options">
 *       <option name="header" value="true" />
 *       <option name="delimiter" value=";" />
 *       <!-- maximum number of new files to be considered in every trigger (default: no max) -->
 *       <option name="maxFilesPerTrigger" value="16" />
 *       <!-- whether to process the latest new files first, useful when there is a large backlog of files (default: false) -->
 *       <option name="latestFirst" value="true" />
 *       <!-- whether to check new files based on only the filename instead of on the full path (default: false) -->
 *       <option name="fileNameOnly" valiue="true" />
 *       <!-- maximum age of a file that can be found in this directory, before it is ignored -->
 *       <option name="maxFileAge" value="10ms|5s|6m|9h|3d" />
 *       <!-- option to clean up completed files after processing -->
 *       <option name="cleanSource" value="ARCHIVE|DELETE|OFF" />
 *     </property>
 *
 *     <!-- the schema applied to the source. This is mandatory -->
 *     <property name="ddlSchema">
 *       <!-- schema can be embedded here or defined in a file. If both specified, the embedded version is used -->
 *       <definition name="string">id int, name string, amount float ...</definition>
 *       <definition name="file">schema/users.ddl</property>
 *     </property>
 *
 *     <!-- the source location to read from. This is mandatory -->
 *     <property name="path">value1</property>
 *
 *     <!-- Defines the tolerance of how late data can still be processed. The time-typed field must be defined. -->
 *     <property> name="waterMark">
 *       <definition name="timeField">event_time</definition>
 *       <definition name="delayThreshold">10 seconds</definition>
 *     </property>
 *     <!-- to add an additional timestamp in the name of __timestamp -->
 *     <property name="addTimestamp">false</property>
 *   </actor>
 */
class FileStreamReader extends Actor {
  //the file format
  @PropertyKey("format", false)
  private var _format: Option[String] = None
  //the options for loading the file
  @PropertyKey("options", true)
  private var _options: Map[String, String] = Map.empty[String, String]
  //the file location
  @PropertyKey("path", false)
  private var _path: Option[String] = None

  //the schema of the target data-frame
  private var _schema: Option[StructType] = None

  //water-mark time field
  @PropertyKey("waterMark.timeField", false)
  private var _wmTimeField: Option[String] = None
  //water-mark delay duration
  @PropertyKey("waterMark.delayThreshold", false)
  private var _wmDelayThreshold: Option[String] = None

  //add timestamp
  @PropertyKey("addTimestamp", false)
  private var _addTimestamp: Boolean = false

  /**
   * Run the file-reader
   *
   * @param ctx - the execution context
   * @param session - the spark-session
   * @return
   */
  override def run(ctx: ExecutionContext)(implicit session: SparkSession): Option[DataFrame] = for {
    _ <- validate(this._format, "The format in FileStreamReader is mandatory or its value is invalid.", Seq("csv", "json", "parquet", "avro", "orc"))
    fmt <- this._format
    _ <- validate(this._schema, "The schema for FileStreamReader is mandatory.")
    schema <- this._schema
    _ <- validate(this._path, "The path in FileStreamReader is mandatory.")
    uri <- this._path
  } yield Try {
    val df = this._options.foldLeft(session.readStream.format(fmt))((s, o) => s.option(o._1, o._2)).schema(schema).load(uri)
    val dfResult = if (this._addTimestamp) df.withColumn("__timestamp", current_timestamp) else df
    //enable water-mark if required
    (this._wmTimeField, this._wmDelayThreshold) match {
      case (Some(m), Some(t)) => dfResult.withWatermark(m, t)
      case _ => dfResult
    }
  } match {
    case Success(df) => df
    case Failure(ex) => throw new RuntimeException(s"Cannot load the file into data-frame - ${_path}.", ex)
  }

  /**
   * Initialize the file reader from the xml definition
   *
   * @param config     - the configuration object
   * @param session    - the spark-session object
   */
  override def init(definition: NodeSeq, config: Config)(implicit session: SparkSession): Unit = {
    super.init(definition, config)

    //check schema
    (this.getProperty[String]("ddlSchema.string"), this.getProperty[String]("ddlSchema.file")) match {
      case (Some(ss), _) => this._schema = Try(StructType.fromDDL(ss)) match {
        case Success(s) => Some(s)
        case Failure(t) => throw new RuntimeException(s"The schema [$ss] is not in valid DDL format.", t)
      }
      case (_, Some(fs)) => this._schema = Try(StructType.fromDDL(FileChannel.loadAsString(fs))) match {
        case Success(s) => Some(s)
        case Failure(t) => throw new RuntimeException(s"The schema is not in valid the DDL format - $fs", t)
      }
      case _ =>
    }
    validate(this._format, "The format in FileStreamReader is mandatory or its value is invalid.", Seq("csv", "json", "parquet", "avro", "orc"))
    validate(this._schema, "The schema in FileStreamReader is mandatory.")
    validate(this._path, "The path in FileStreamReader is mandatory.")
  }

  /**
   * The format of the source files.
   *
   * @param fmt
   * @return
   */
  def sourceFormat(fmt: String): FileStreamReader = { this._format = Some(fmt); this }

  /**
   * The load option
   *
   * @param name
   * @param value
   * @return
   */
  def readOption(name: String, value: String): FileStreamReader = { this._options = this._options + (name -> value); this }

  /**
   * The load options
   *
   * @param opts
   * @return
   */
  def readOptions(opts: Map[String, String]): FileStreamReader = { this._options = this._options ++ opts; this }

  /**
   * The schema of the target data-frame
   *
   * @param schema
   * @return
   */
  def ddlSchema(schema: StructType): FileStreamReader = { this._schema = Some(schema); this }

  /**
   * The source path of the files
   *
   * @param dir
   * @return
   */
  def sourcePath(dir: String): FileStreamReader = { this._path = Some(dir); this }

  /**
   * Specify the water-mark time field
   *
   * @param field
   * @return
   */
  def watermarkTimeField(field: String): FileStreamReader = { this._wmTimeField = Some(field); this }

  /**
   * Specify teh water-mark delay threshold
   *
   * @param duration
   * @return
   */
  def watermarkDelayThreshold(duration: String): FileStreamReader = { this._wmDelayThreshold = Some(duration); this }

  /**
   * Flag of whether or not to add __timestamp with current timestamp.
   *
   * @param value
   * @return
   */
  def addTimestamp(value: Boolean = false): FileStreamReader = { this._addTimestamp = value; this }
}
