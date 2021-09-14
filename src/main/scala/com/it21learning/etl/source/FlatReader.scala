package com.it21learning.etl.source

import com.it21learning.etl.common.{Actor, ExecutionContext}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import scala.util.{Failure, Success, Try}
import com.it21learning.common.PropertyKey
import com.it21learning.common.io.FileChannel
import com.typesafe.config.Config
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.{StringType, StructField, StructType}

/**
 * To load a text file.
 *
 * The following is the definition in xml format
 *
 * <actor type="com.it21learning.etl.source.FlatReader">
 *   <!-- the source location to read from. This is mandatory -->
 *   <property name="path">hdfs://...</property>
 * </actor>
 *
 * The output dataframe has the following columns:
 *   - row_value: the content of each row
 *   - row_no: the sequence number of each row.
 */
final class FlatReader() extends Actor {
  //the schema for flat-rows
  private case class FlatField(name: String, startPos: Int, length: Int, typ: String)

  @PropertyKey("ddlFieldsString", false)
  private var _ddlFieldsString: Option[String] = None
  @PropertyKey("ddlFieldsFile", false)
  private var _ddlFieldsFile: Option[String] = None

  //the source path
  @PropertyKey("fileUri", true)
  private var _fileUri: Option[String] = None

  //format
  private var _format: Seq[FlatField] = Nil
  //schema
  private var _schema: Option[StructType] = None

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
    val rdd = session.sparkContext.textFile(uri)
    this._format match {
      case Seq(_, _ @ _*) =>
        val rawRdd = rdd.map(r => this._format.map(f => r.substring(f.startPos - 1, f.startPos + f.length - 1))).map(r => Row.fromSeq(r))
        val rawSchema = StructType(this._schema.get.fields.map(field => StructField(field.name, StringType, field.nullable)))
        session.createDataFrame(rawRdd, rawSchema)
          .select(this._schema.get.fields.map(field => col(field.name).cast(field.dataType)): _*)
      case _ => session.sparkContext.textFile(uri).zipWithIndex.toDF("row_value", "row_no")
    }
  } match {
    case Success(df) => df
    case Failure(ex) => throw new RuntimeException(s"Cannot load the flat file - $uri", ex)
  }

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
   * The source path of the files
   *
   * @param path
   * @return
   */
  def sourcePath(path: String): FlatReader = { this._fileUri = Some(path); this }
}
