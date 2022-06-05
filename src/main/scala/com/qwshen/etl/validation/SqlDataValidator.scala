package com.qwshen.etl.validation

import com.qwshen.common.PropertyKey
import com.qwshen.etl.common.{Actor, JobContext}
import com.typesafe.config.Config
import org.apache.spark.sql.{DataFrame, SparkSession}
import scala.util.{Failure, Success, Try}

/**
 * Validate data with validWhere or invalidWhere to filter out valid or invalid records.
 */
class SqlDataValidator() extends Actor {
  @PropertyKey("validWhere", false)
  protected var _validWhere: Option[String] = None
  @PropertyKey("invalidWhere", false)
  protected var _invalidWhere: Option[String] = None

  @PropertyKey("action", true)
  protected var _action: Option[String] = None

  @PropertyKey("staging.uri", false)
  protected var _stagingUri: Option[String] = None
  @PropertyKey("staging.format", false)
  protected var _stagingFormat: Option[String] = None
  @PropertyKey("staging.options.*", false)
  protected var _stagingOptions: Map[String, String] = Map.empty[String, String]

  @PropertyKey("view", true)
  protected var _view: Option[String] = None

  /**
   * Run the actor
   *
   * @param ctx - the execution context
   * @param session - the spark-session
   * @return
   */
  def run(ctx: JobContext)(implicit session: SparkSession): Option[DataFrame] = for {
    df <- this._view.flatMap(name => ctx.getView(name))
    action <- this._action
  } yield Try {
    val (dfValid: DataFrame, dfInvalid: DataFrame) = (this._validWhere, this._invalidWhere) match {
      case (Some(valid), _) => (df.filter(valid), df.filter(s"not ($valid)"))
      case (_, Some(invalid)) => (df.filter(s"not ($invalid)"), df.filter(invalid))
      case _ => throw new RuntimeException("The validWhere and invalidWhere cannot be both empty.")
    }
    action match {
      case "error" => if (dfInvalid.count() > 0) throw new RuntimeException("There are invalid data.")
      case "staging" => for {
        uri <- this._stagingUri
        fmt <- this._stagingFormat
      } {
        dfInvalid.cache
        if (dfInvalid.count() > 0) {
          this._stagingOptions.foldLeft(dfInvalid.write.format(fmt))((w, o) => w.option(o._1, o._2)).save(uri)
        }
      }
      case "ignore" =>
    }
    dfValid
  } match {
    case Success(df) => df
    case Failure(ex) => throw new RuntimeException("Failed when validating data", ex)
  }

  /**
   * Initialize the SqlDataValidator reader
   */
  override def init(properties: Seq[(String, String)], config: Config)(implicit session: SparkSession): Unit = {
    super.init(properties, config)

    this.validate(Seq(this._validWhere, this._invalidWhere), "The validWhere and invalidWhere cannot be both empty.")
    this._action match {
      case Some(v) if v == "staging" =>
        this.validate(Seq(this._stagingUri, this._stagingFormat), "The staging uri & format are mandatory since the action is to stage in SqlDataValidator.")
        this.validate(this._stagingFormat, "The staging format must be either csv, json, parquet, avro.", Seq("csv", "json", "parquet", "avro"))
      case _ =>
    }
    this.validate(this._action, "The action must be either error, staging or ignore", Seq("error", "staging", "ignore"))
  }

  /**
   * The where clouse for identifying valid records
   * @param where
   * @return
   */
  def validWhere(where: String): SqlDataValidator = { this._validWhere = Some(where); this }
  /**
   * The where clouse for identifying invalid records
   * @param where
   * @return
   */
  def invalidWhere(where: String): SqlDataValidator = { this._invalidWhere = Some(where); this }

  /**
   * The action if invalid data found - must be one of error, staging or ignore
   * @param value
   * @return
   */
  def action(value: String): SqlDataValidator = { this._action = Some(value); this }

  /**
   * The uri for staging invalid records
   * @param uri
   * @return
   */
  def stagingUri(uri: String): SqlDataValidator = { this._stagingUri = Some(uri); this }
  /**
   * The format for staging invalid records
   * @param format
   * @return
   */
  def stagingFormat(fmt: String): SqlDataValidator = { this._stagingFormat = Some(fmt); this }
  /**
   * The staging option
   *
   * @param name
   * @param value
   * @return
   */
  def stagingOption(name: String, value: String): SqlDataValidator = { this._stagingOptions = this._stagingOptions + (name -> value); this }
  /**
   * The staging options
   *
   * @param opts
   * @return
   */
  def stagingOptions(opts: Map[String, String]): SqlDataValidator = { this._stagingOptions = this._stagingOptions ++ opts; this }

  /**
   * The view to be validated.
   *
   * @param view
   * @return
   */
  def forView(view: String): SqlDataValidator = { this._view = Some(view); this }
}
