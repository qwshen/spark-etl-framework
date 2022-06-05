package com.qwshen.etl.common

import com.qwshen.common.logging.Loggable
import com.qwshen.etl.pipeline.definition.View
import com.typesafe.config.Config
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * The execution context
 *
 * @param session
 * @param appCtx
 */
class JobContext private[etl](val appCtx: PipelineContext, val config: Option[Config] = None)(implicit session: SparkSession) extends Loggable {
  //the _container for holding any object between actions
  private val _container: scala.collection.mutable.Map[String, Any] = scala.collection.mutable.Map.empty[String, Any]

  /**
   * Add an object into the _container
   *
   * @param key
   * @param obj
   * @return
   */
  def addObject(key: String, obj: Any): Unit = this._container.put(key, obj)

  /**
   * Get an object by name from the _container
   * @param key
   * @return
   */
  def getObject(key: String): Option[Any] = this._container.get(key)

  /**
   * Get a value in the configuration by the key
   *
   * @param key
   * @return
   */
  def getConfigValue(key: String): Option[AnyRef] = this.config.map(cfg => cfg.getAnyRef(key))

  /**
   * Check if a view exists
   *
   * @param name
   * @return
   */
  def viewExists(name: String): Boolean = this.session.catalog.tableExists(name)
  def viewExists(view: View): Boolean = this.session.catalog.tableExists(if (view.global) s"${appCtx.global_db}.${view.name}" else view.name)

  /**
   * Extract the view by the name
   *
   * @param name
   * @return
   */
  def getView(name: String): Option[DataFrame] = if (viewExists(name)) { Some(this.session.table(name)) } else {
    this.logger.warn(s"The view [${name}] doesn't exist.")
    None
  }
  def getView(view: View): Option[DataFrame] = getView(if (view.global) s"${appCtx.global_db}.${view.name}" else view.name)

  //metrics collection hint
  private var _metricsRequired: Boolean = false
  /**
   * Check this flag to see if metrics collection is required
   * @return
   */
  def metricsRequired = this._metricsRequired
  //this method is only called from pipeline-runner
  private[etl] def metricsRequired_= (newVal: Boolean) = this._metricsRequired = newVal
}
