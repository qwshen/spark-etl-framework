package com.qwshen.etl.common

import com.qwshen.common.{PropertyInitializer, PropertyStatus, PropertyValidator}
import com.qwshen.etl.pipeline.definition.View
import com.typesafe.config.Config
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.collection.breakOut
import scala.util.{Failure, Success, Try}
import scala.collection.mutable.ArrayBuffer

/**
 * The common behavior of all Actors
 */
abstract class Actor extends PropertyInitializer with PropertyValidator with Serializable {
  private final val _keySqlVariables = "qwshen.s__q_l.v___ar__i_ables____"
  //to record the variables that don't have value assigned yet during the initialization phase
  private var _unassignedVariables: Seq[String] = Nil

  //to record all extra-views that current actor handles
  private val _extraViews = ArrayBuffer.empty[View]
  /**
   * Get all extra-views
   * @return
   */
  def extraView: Seq[View] = this._extraViews.toSeq

  /**
   * Initialize the actor with the properties & config
   *
   * @param properties
   * @param config
   */
  def init(properties: Seq[(String, String)], config: Config)(implicit session: SparkSession): Unit = {
    val propertiesStatus = this.init(config) ++ this.init(properties)

    def merge(x: Option[Any], y: Option[Any]): Option[Any] = if (x.nonEmpty) x else y
    def combine(path: String, ps: Seq[PropertyStatus]): PropertyStatus = ps.reduce((x, y) => PropertyStatus(path, merge(x.value, y.value), x.applied || y.applied))
    val props: Seq[PropertyStatus] = propertiesStatus.map(p => (p.path, p)).groupBy(_._1).map { case(path, ps) => combine(path, ps.map(p => p._2)) } (breakOut)

    if (props.exists(!_.applied)) {
      this._unassignedVariables = props.filter(!_.applied).map(p => p.path)
      val sqlVariables = this.getSqlVariables
      val keys = this._unassignedVariables.map(uv => (uv, sqlVariables.exists(sv => uv.equals(uv)))).filter(!_._2).map(_._1).mkString(",")
      if (keys.nonEmpty) {
        throw new RuntimeException(s"For ${this.getClass.getCanonicalName}, the properties with key(s) [$keys] required, but its(their) value(s) not provided.")
      }
    }

    if (this.logger.isDebugEnabled) {
      logger.info("Properties for " + this.getClass.getCanonicalName + ":")
      props.filter(_.applied).foreach(p => logger.info(p.path + " = " + p.value))
    }
  }

  /**
   * Called by pipeline-runner before calling the run method
   * @param session - the spark-session object
   */
  private[etl] def beforeRun(implicit session: SparkSession): Unit = if (this._unassignedVariables.nonEmpty) {
    val props = this._unassignedVariables.map(vn => Try {
      this.getSqlVariableValue(vn)
    } match {
      case Success(v) => (vn, v)
      case Failure(e) => throw new RuntimeException(s"For ${this.getClass.getCanonicalName}, the property with key [$vn] doesn't have a value defined.")
    })
    this.init(props)
  }

  /**
   * Run the actor
   * @param ctx     - the execution context
   * @param session - the spark-session
   * @return
   */
  def run(ctx: JobContext)(implicit session: SparkSession): Option[DataFrame]

  /**
   * Retrieve sql-variables defined by set statements
   * @param session - the spark-session object
   * @return - all sql variables defined in current session
   */
  private[etl] def getSqlVariables(implicit session: SparkSession): Seq[String] = Try {
    import session.implicits._
    session.sql(String.format("select ${%s}", this._keySqlVariables)).as[String].first
  }match {
    case Success(s) => s.split(";")
    case _ => Nil
  }

  /**
   * Get the value of a sql-variable
   * @param varName - the name of the sql-variable
   * @param session - the spark-session object
   * @return - the value of the sql-variable
   */
  private def getSqlVariableValue(varName: String)(implicit session: SparkSession): String = {
    import session.implicits._
    session.sql(String.format("select ${%s}", varName)).as[String].first()
  }

  /**
   * Set combined sql-variables into a system-variable
   * @param variables - all sql-variables
   * @param session - the spark-session object
   */
  private[etl] def setSqlVariables(variables: Seq[String])(implicit session: SparkSession): Unit = {
    session.sql(String.format("set %s = `'%s'`", this._keySqlVariables, variables.mkString(";")))
  }

  /**
   * Collect metrics of current actor
   * @param df
   * @param session
   * @return
   */
  def collectMetrics(df: Option[DataFrame]): Seq[(String, String)] = Nil

  /**
   * Register extra view handled by current actor
   * @param df
   * @param viewName
   * @param global
   */
  protected def registerView(df: DataFrame, view: View): Unit = {
    if (view.global) df.createOrReplaceGlobalTempView(view.name) else df.createOrReplaceTempView(view.name)
    this._extraViews.append(view)
  }
  protected def registerView(df: DataFrame, viewName: String, global: Boolean): Unit = this.registerView(df, View(viewName, global))
}
