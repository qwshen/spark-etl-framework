package com.it21learning.etl.validation

import com.it21learning.common.io.FileChannel
import com.it21learning.etl.common.{Actor, ExecutionContext}
import com.typesafe.config.Config
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.types.{DataType, StructField, StructType}
import org.apache.spark.sql.functions.col
import scala.util.{Failure, Success, Try}
import scala.xml.NodeSeq

/**
  * This class is for validating a data-frame with schema
  *
  * The following is the definition in xml format
  *
  * <actor type="com.it21learning.etl.validation.SchemaValidator">
  *   <!--
  *    One of the following options must be defined. Only the last definition is used if multiple schemas defined.
 *     This is mandatory.
  *   -->
  *   <property name="jsonSchemaString">{"type":"struct","fields":[{"name":"user","type":"string","nullable":true},{"name":"event","type":"string","nullable":true}]}</property>
  *   <!-- or -->
  *   <property name="jsonSchemaFile">schemas/users.schema</property>
  *
  *   <!-- The action defines how to proceed if the validation fails. This is mandatory -->
  *   <property name="action">errorOut|logWarning</property>

  *   <!-- The target view to be validated by the schema. This is mandatory -->
  *   <property name="view">events</property>
  * </actor>
  */
final class SchemaValidator extends Actor {
  //the schema from definition
  private var _schema: Option[StructType] = None
  //the action
  private var _action: Option[String] = None

  //the target view to be validated
  private var _view: Option[String] = None

  /**
    * Run the file-reader
    *
    * @param ctx - the execution context
    * @param session - the spark-session
    * @return
    */
  def run(ctx: ExecutionContext)(implicit session: SparkSession): Option[DataFrame] = for {
    _ <- validate(this._schema, "The schema in SchemaValidator is mandatory.")
    schema <- this._schema
    _ <- validate(this._view, "The view in SchemaValidator is mandatory.")
    df <- _view.flatMap(name => ctx.getView(name))
    _ <- validate(this._action, "The action in SchemaValidator is mandatory or its value is invalid.", Seq("errorOut", "logWarning"))
    action <- this._action
  } yield Try {
    //validate if the schemas are matched
    val expectedFields: Seq[StructField] = schema.fields.sortWith((x, y) => x.name > y.name)
    val actualFields: Seq[StructField] = df.schema.fields.sortWith((x, y) => x.name > y.name)

    //check if the # of fields matches
    if (expectedFields.length != actualFields.length) {
      throw new RuntimeException("The # of fields from both schema is not matched.")
    }
    //apply the expected schema to the data-frame
    val dfChanged = df.select(expectedFields.map(f => col(f.name).cast(f.dataType)): _*)

    //validate data - if a field is not nullable but with null value.
    val dfResult = actualFields.filter(f => !f.nullable).foldLeft(dfChanged)((r, f) => r.filter(col(f.name).isNull))
    //cache df if needed
    if (!(df.storageLevel.useMemory || df.storageLevel.useDisk || df.storageLevel.useOffHeap)) {
      df.cache
    }
    if (dfResult.count() > 0) {
      throw new RuntimeException("The non-nullable fields have null value.")
    }
    dfResult
  } match {
    case Success(dfResult) => dfResult
    case Failure(ex) => if (action == "errorOut") {
      //error out
      throw new RuntimeException("The data validation failed with the specified schema.", ex)
    }
    else {
      //logging
      this.logger.warn(s"The data validation failed with the specified schema. - ${ex.getStackTrace.mkString("Array(", ", ", ")")}")
      df
    }
  }

  /**
    * Initialize the file reader from the xml definition
    *
    * @param config     - the configuration object
    * @param session    - the spark-session object
    */
  override def init(definition: NodeSeq, config: Config)(implicit session: SparkSession): Unit = {
    super.init(config)

    (definition \ "property").foreach(prop => (prop \ "@name").text match {
      case "jsonSchemaString" => jsonSchemaString(prop.text)
      case "jsonSchemaFile" => jsonSchemaFile(prop.text)
      case "action" => this._action = Some(prop.text)
      case "view" => this._view = Some(prop.text)
      case _ =>
    })

    validate(this._schema, "The schema in SchemaValidator is mandatory.")
    validate(this._action, "The action in SchemaValidator is mandatory or its value is invalid.", Seq("errorOut", "logWarning"))
    validate(this._view, "The view in SchemaValidator is mandatory.")
  }

  /**
   * The json schema in string
   * @param value
   * @return
   */
  def jsonSchemaString(value: String): SchemaValidator = {
    Try(DataType.fromJson(value).asInstanceOf[StructType]) match {
      case Success(s) => Some(s)
      case Failure(t) => throw new RuntimeException(s"The schema [${value}] is not in valid DDL format.", t)
    }
    this
  }

  /**
   * The json schema file
   * @param file
   * @return
   */
  def jsonSchemaFile(file: String): SchemaValidator = {
    Try(DataType.fromJson(FileChannel.loadAsString(file)).asInstanceOf[StructType]) match {
      case Success(s) => Some(s)
      case Failure(t) => throw new RuntimeException(s"The schema is not in valid the DDL format - ${file}", t)
    }
    this
  }

  /**
   * The action will be taken if errors
   *
   * @param value
   * @return
   */
  def action(value: String): SchemaValidator = { this._action = Some(value); this }

  /**
   * The view to be validated.
   *
   * @param view
   * @return
   */
  def forView(view: String): SchemaValidator = { this._view = Some(view); this }
}
