package com.qwshen.etl.validation

import com.qwshen.common.PropertyKey
import com.qwshen.common.io.FileChannel
import com.qwshen.etl.common.{Actor, ExecutionContext}
import com.typesafe.config.Config
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.types.{DataType, StructField, StructType}
import org.apache.spark.sql.functions.{lit, col}
import scala.collection.breakOut
import scala.util.{Failure, Success, Try}

/**
  * This class is for validating the schema of theinput data-frame with schema provided
  */
class SchemaValidator extends Actor {
  @PropertyKey("ddlSchemaString", false)
  protected var _ddlSchemaString: Option[String] = None
  @PropertyKey("ddlSchemaFile", false)
  protected var _ddlSchemaqFile: Option[String] = None

  @PropertyKey("type", true)
  protected var _type: Option[String] = None
  @PropertyKey("mode", true)
  protected var _mode: Option[String] = None
  @PropertyKey("action", true)
  protected var _action: Option[String] = None

  //the target view to be validated
  @PropertyKey("view", true)
  protected var _view: Option[String] = None

  //the schema from definition
  private var _schema: Option[StructType] = None

  /**
    * Run the file-reader
    *
    * @param ctx - the execution context
    * @param session - the spark-session
    * @return
    */
  def run(ctx: ExecutionContext)(implicit session: SparkSession): Option[DataFrame] = for {
    schema <- this._schema
    df <- this._view.flatMap(name => ctx.getView(name))
    vt <- this._type
    md <- this._mode
    action <- this._action
  } yield Try {
    val same = (f1: StructField, f2: StructField) => f1.name.equals(f2.name) && f1.dataType == f2.dataType
    vt match {
      case "match" =>
        val (s1: StructType, s2: StructType) = md match {
          case "strict" => (schema, df.schema)
          case _ => (schema.fields.sortWith((x, y) => x.name > y.name), df.schema.fields.sortWith((x, y) => x.name > y.name))
        }
        val error: Option[String] = if (s1.fields.length != s2.fields.length) {
          Some("The number of columns from both schemas doesn't match.")
        } else {
          if (schema.fields.indices.forall(i => same(schema.fields(i), df.schema.fields(i)))) {
            None
          } else {
            Some("Either the name or data-type from both schemas doesn't match.")
          }
        }
        if (action.equals("error")) {
          error.foreach(e => throw new RuntimeException(e))
        } else {
          error.foreach(e => logger.warn(e))
        }
        df

      case "adapt" =>
        val m1: Map[String, DataType] = schema.fields.sortWith((x, y) => x.name > y.name).map(x => (x.name, x.dataType))(breakOut)
        val m2: Map[String, DataType] = df.schema.fields.sortWith((x, y) => x.name > y.name).map(x => (x.name, x.dataType))(breakOut)
        val error: Option[String] = if (m2.forall { case (k, v) => m1.contains(k) && v == m1(k) }) {
          None
        } else {
          Some("Columns(s) from the dataframe not included in the target schema.")
        }
        if (md.equals("strict") && action.equals("error")) {
          error.foreach(e => throw new RuntimeException(e))
        } else {
          error.foreach(e => logger.warn(e))
        }
        df.select(schema.fields.map(f => if (m2.contains(f.name)) col(f.name) else lit(null).cast(f.dataType)): _*)
    }
  } match {
    case Success(dfResult) => dfResult
    case Failure(ex) => throw new RuntimeException("The data validation failed with the specified schema.", ex)
  }

  /**
   * Initialize the kafka reader
   */
  override def init(properties: Seq[(String, String)], config: Config)(implicit session: SparkSession): Unit = {
    super.init(properties, config)

    (this._ddlSchemaString, this._ddlSchemaqFile) match {
      case (Some(s), _) => this._schema = Some(StructType.fromDDL(s))
      case (_, Some(f)) => this._schema = Some(StructType.fromDDL(FileChannel.loadAsString(f)))
      case _ => throw new RuntimeException("The schema must be provided by either ddlSchemaString or ddlSchemaFile.")
    }

    validate(this._type, "The validation type in SchemaValidator must be either match or adapt", Seq("match", "adapt"))
    validate(this._mode, "The validation mode in SchemaValidator must be either strict or default.", Seq("strict", "default"))
    validate(this._action, "The action for the validation must be either error or ignore", Seq("error", "ignore"))
  }

  /**
   * The json schema in string
   * @param value
   * @return
   */
  def schema(value: StructType): SchemaValidator = { this._schema = Some(value); this }

  /**
   * The type of validation
   *
   * @param value
   * @return
   */
  def validationType(value: String): SchemaValidator = { this._action = Some(value); this }
  /**
   * The mode for how to compare schema
   *
   * @param value
   * @return
   */
  def validationMode(value: String): SchemaValidator = { this._mode = Some(value); this }
  /**
   * The action will be taken if errors
   *
   * @param value
   * @return
   */
  def validationAction(value: String): SchemaValidator = { this._action = Some(value); this }

  /**
   * The view to be validated.
   *
   * @param view
   * @return
   */
  def forView(view: String): SchemaValidator = { this._view = Some(view); this }
}
