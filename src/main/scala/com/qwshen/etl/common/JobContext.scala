package com.qwshen.etl.common

import com.qwshen.common.logging.Loggable
import com.qwshen.etl.pipeline.definition.View
import com.typesafe.config.Config
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.storage.StorageLevel
import scala.util.{Success, Try}

/**
 * The execution context
 *
 * @param session
 * @param appCtx
 */
case class JobContext private[etl](val appCtx: PipelineContext, val config: Option[Config] = None)(implicit session: SparkSession) extends Loggable {
  //the _container for holding any object between actions
  private val _container: scala.collection.mutable.Map[String, Any] = scala.collection.mutable.Map.empty[String, Any]

  //key for holding all referenced views in the current session
  private val _keyViews: String = "qwshen.ref__k_e__ys____"
  //the _refViews for holding all referenced views in the current session
  private val _refViews: scala.collection.mutable.Map[String, Int] = this.loadViews()

  /**
   * Load referenced views from the current session
   * @return
   */
  private def loadViews(): scala.collection.mutable.Map[String, Int] = Try {
    import session.implicits._
    session.sql(s"select ${this._keyViews}").as[String].first.split(";").map(x => x.split(":")).map(x => (x(0), x(1).toInt)).toSeq
  } match {
    case Success(m) => scala.collection.mutable.Map(m: _*)
    case _ => scala.collection.mutable.Map.empty[String, Int]
  }

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
   * Flag a view having been referenced
   * @param name - the name of the view
   */
  def viewReferenced(name: String): Unit = {
    //if the view is global or referenced multiple times, cache it
    this._refViews.get(name) match {
      case Some(count) if count > 1 => this.getView(name) match {
        case Some(df) if !df.isStreaming && !(df.storageLevel.useMemory || df.storageLevel.useDisk || df.storageLevel.useOffHeap) => df.persist(StorageLevel.MEMORY_AND_DISK)
        case _ =>
      }
      case _ =>
    }
    //add the counter
    this._refViews.put(name, this._refViews.getOrElse(name, 0) + 1)
  }

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
  def metricsRequired: Boolean = this._metricsRequired
  //this method is only called from pipeline-runner
  private[etl] def metricsRequired_= (newVal: Boolean): Unit = this._metricsRequired = newVal

  /**
   * Dispose the context
   */
  def dispose(): Unit = session.sql(String.format("set %s = `'%s'`", this._keyViews, this._refViews.map(x => String.format("%s:%s", x._1, x._2.toString)).mkString(";")))
}
