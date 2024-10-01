package com.qwshen.etl.common

import org.apache.spark.sql.SparkSession
import com.qwshen.common.PropertyKey
import com.qwshen.common.io.FileChannel
import com.typesafe.config.Config
import org.apache.spark.sql.types.{StringType, StructField, StructType}

/**
 * The FlatReadActor is a common class for FlatReader & FlatStreamReader.
 */
private[etl] abstract class FlatReadActor[T] extends Actor { self: T =>
  @PropertyKey("ddlFieldsString", false)
  protected var _ddlFieldsString: Option[String] = None
  @PropertyKey("ddlFieldsFile", false)
  protected var _ddlFieldsFile: Option[String] = None

  @PropertyKey("options.*", false)
  protected var _options: Map[String, String] = Map.empty[String, String]

  @PropertyKey("row.valueField", false)
  protected var _valueField: String = "value"
  //default schema
  protected var _defaultSchema: StructType = _

  //the source path
  @PropertyKey("fileUri", true)
  protected var _fileUri: Option[String] = None

  //body schema
  protected var _schema: Option[StructType] = None

  /**
   * Initialize the flat reader
   *
   * @param config - the configuration object
   * @param session - the spark-session object
   */
  override def init(properties: Seq[(String, String)], config: Config)(implicit session: SparkSession): Unit = {
    super.init(properties, config)

    this._defaultSchema = StructType(Seq(StructField(this._valueField, StringType)))
    validate(this._fileUri, "The fileUri in FlatReader is mandatory.")
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
  def valueField(field: String): T = { this._valueField = field; this }

  /**
   * The source path of the files
   *
   * @param path
   * @return
   */
  def fileUri(path: String): T = { this._fileUri = Some(path); this }
}

private[etl] object FlatReadActor {
  //the schema for positional-rows
  case class PositionalField(name: String, startPos: Int, length: Int, typ: String)
  //the schema for delimited-rows
  case class DelimitedField(name: String, index: Int, typ: String)

  /**
   * Parse a positional DDL string in the format of:
   *   user:1-8 string, event:9-10 long, timestamp:19-32 string, interested:51-1 int
   *
   * @param ddlString - the positional ddl string passed as string
   * @param ddlFile - the positional ddl string is provided from a file
   * @return
   */
  def parsePositionalFields(ddlString: Option[String], ddlFile: Option[String]): (Option[StructType], Seq[FlatReadActor.PositionalField]) = {
    //prepare schema
    if (ddlString.isEmpty && ddlFile.isEmpty)
      throw new RuntimeException("The fields ddl-string is not provided.")

    try {
      val posFields = (if (ddlString.nonEmpty) ddlString else ddlFile.map(f => FileChannel.loadAsString(f))).map(
        _.replaceAll("[\r|\n]", "")
        .split(",").map(_.trim.split(" "))
        .map(x => (x(0).split(":"), x(1))).map(x => (x._1(0).trim, x._1(1).split("-"), x._2.trim))
        .map(x => PositionalField(x._1, x._2(0).toInt, x._2(1).toInt, x._3)).toSeq
      )
      (posFields.map(fields => createSchema(fields.map(field => (field.name, field.typ)))), posFields.getOrElse(Nil))
    } catch {
      case e: Exception => throw new RuntimeException("The fields ddl-string is not valid in FlatReader.", e)
    }
  }

  /**
   * Parse a delimited DDL string in the format of:
   *   user:0 string, event:2 long, timestamp:5 string, interested:7 int
   *
   * @param ddlString - the delimited ddl string passed as string
   * @param ddlFile - the delimited ddl string is provided from a file
   * @return
   */
  def parseDelimitedFields(ddlString: Option[String], ddlFile: Option[String]): (Option[StructType], Seq[FlatReadActor.DelimitedField]) = {
    //prepare schema
    if (ddlString.isEmpty && ddlFile.isEmpty)
      throw new RuntimeException("The fields ddl-string is not provided.")

    try {
      val dlmFields = (if (ddlString.nonEmpty) ddlString else ddlFile.map(f => FileChannel.loadAsString(f))).map(
        _.replaceAll("[\r|\n]", "")
          .split(",").map(_.trim.split(" "))
          .map(x => (x(0).split(":"), x(1))).map(x => (x._1(0).trim, x._1(1), x._2.trim))
          .map(x => DelimitedField(x._1, x._2.toInt, x._3)).toSeq
      )
      (dlmFields.map(fields => createSchema(fields.map(field => (field.name, field.typ)))), dlmFields.getOrElse(Nil))
    } catch {
      case e: Exception => throw new RuntimeException("The fields ddl-string is not valid in FlatReader.", e)
    }
  }

  //create schema from positional-fields
  private def createSchema(fields: Seq[(String, String)]): StructType = {
    StructType.fromDDL(fields.map(f => String.format("%s %s", f._1, f._2)).fold("")((r, m) => if (r.nonEmpty) r + "," + m else m))
  }
}