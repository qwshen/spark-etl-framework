package com.qwshen.etl.common

import org.apache.spark.sql.{DataFrame, SparkSession}
import com.qwshen.common.PropertyKey

/**
 * Set up spark-settings
 */
class SparkConfActor extends Actor {
  @PropertyKey("configs.*", false)
  protected var _settings: Map[String, String] = Map.empty[String, String]

  @PropertyKey("hadoopConfigs.*", false)
  protected var _hadoopSettings: Map[String, String] = Map.empty[String, String]

  /**
   * Setting up the settings
   *
   * @param session
   * @return
   */
  def run(ctx: JobContext)(implicit session: SparkSession): Option[DataFrame] = {
    this._settings.foreach { case (k, v) => session.conf.set(k, v) };
    this._hadoopSettings.foreach { case(k, v) => session.sparkContext.hadoopConfiguration.set(k, v) }
    None
  }

  /**
   * Add a setting
   *
   * @param name
   * @param value
   * @return
   */
  def add(name: String, value: String): SparkConfActor = { this._settings = this._settings + (name -> value); this }
  /**
   * Add a collection of setting
   * @param settings
   * @return
   */
  def add(settings: Map[String, String]): SparkConfActor = { this._settings = this._settings ++ settings; this }

  /**
   * Add a hadoop setting
   * @param name
   * @param value
   * @return
   */
  def addHadoop(name: String, value: String): SparkConfActor = { this._hadoopSettings = this._hadoopSettings + (name -> value); this }
  /**
   * Add a collection of hadoop setting
   * @param settings
   * @return
   */
  def addHadoop(settings: Map[String, String]): SparkConfActor = { this._hadoopSettings = this._hadoopSettings ++ settings; this }
}
