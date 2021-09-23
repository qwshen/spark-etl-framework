package com.it21learning.etl.common

import org.apache.spark.sql.SparkSession
import scala.util.{Failure, Success, Try}
import com.it21learning.common.PropertyKey
import com.it21learning.common.io.FileChannel
import com.typesafe.config.Config
import org.apache.spark.sql.types.StructType

/**
 * The FlatReadActor is a common class for FlatReader & FlatStreamReader.
 */
private[etl] abstract class FlatReadActor[T] extends Actor { self: T =>
  //the schema for flat-rows
  protected case class FlatField(name: String, startPos: Int, length: Int, typ: String)

  //the options for loading the file
  @PropertyKey("options.*", false)
  protected var _options: Map[String, String] = Map.empty[String, String]

  @PropertyKey("ddlFieldsString", false)
  protected var _ddlFieldsString: Option[String] = None
  @PropertyKey("ddlFieldsFile", false)
  protected var _ddlFieldsFile: Option[String] = None

  @PropertyKey("row.valueField", false)
  protected var _valueField: Option[String] = None

  //the source path
  @PropertyKey("fileUri", true)
  protected var _fileUri: Option[String] = None

  //format
  protected var _format: Seq[FlatField] = Nil
  //schema
  protected var _schema: Option[StructType] = None

  /**
   * Initialize the flat reader
   *
   * @param config - the configuration object
   * @param session - the spark-session object
   */
  override def init(properties: Seq[(String, String)], config: Config)(implicit session: SparkSession): Unit = {
    super.init(properties, config)

    //prepare schema
    try {
      (if (this._ddlFieldsString.nonEmpty) this._ddlFieldsString else this._ddlFieldsFile.map(f => FileChannel.loadAsString(f))).foreach(
        ss => this._format = ss.replaceAll("[\r|\n]", "")
          .split(",").map(_.trim.split(" "))
          .map(x => (x(0).split(":"), x(1))).map(x => (x._1(0).trim, x._1(1).split("-"), x._2))
          .map(x => FlatField(x._1, x._2(0).toInt, x._2(1).toInt, x._3))
      )
    } catch {
      case e: Exception => throw new RuntimeException("The schema provided is not valid in FlatReader.", e)
    }
    val ddlString = this._format.map(fmt => String.format("%s %s", fmt.name, fmt.typ)).fold("")((r, m) => if (r.nonEmpty) r + "," + m else m)
    this._schema = if (ddlString.nonEmpty) {
      Try(StructType.fromDDL(ddlString)) match {
        case Success(s) => Some(s)
        case Failure(t) => throw new RuntimeException(s"The schema [$ddlString] is not in valid DDL format.", t)
      }
    } else None
    if ((this._format.nonEmpty && this._schema.isEmpty) || (this._format.isEmpty && this._schema.nonEmpty)
      || (this._format.nonEmpty && this._schema.nonEmpty && this._format.length != this._schema.get.fields.length)) {
      throw new RuntimeException("The schema provided is not valid in FlatReader.")
    }
    validate(this._fileUri, "The fileUri in FileReader is mandatory.")
  }

  /**
   * The fields definition
   * @param ddlString
   * @return
   */
  def ddlFieldsString(ddlString: String): T = { this._ddlFieldsString = Some(ddlString); this }

  /**
   * The fields definition from a file.
   * @param ddlFile
   * @return
   */
  def ddlFieldsFile(ddlFile: String): T = { this._ddlFieldsFile = Some(ddlFile); this }

  /**
   * The custom value field name
   * @param fiel
   * @return
   */
  def valueField(field: String): T = { this._valueField = Some(field); this }

  /**
   * The source path of the files
   *
   * @param path
   * @return
   */
  def fileUri(path: String): T = { this._fileUri = Some(path); this }
}
