package com.qwshen.etl.common

import org.apache.spark.sql.{DataFrame, SparkSession}
import com.qwshen.common.PropertyKey

/**
 * Set up spark-settings
 */
final class SparkConfActor(private val settings: Seq[(String, String)]) extends Actor {
  @PropertyKey("configs.*", true)
  private var _settings: Map[String, String] = Map.empty[String, String]

  /**
   * Setting up the settings
   *
   * @param session
   * @return
   */
  def run(ctx: ExecutionContext)(implicit session: SparkSession): Option[DataFrame] = { this._settings.foreach { case (k, v) => session.conf.set(k, v) }; None }

  /**
   * Add a setting
   *
   * @param name
   * @param value
   * @return
   */
  def add(name: String, value: String): SparkConfActor = { this._settings = this._settings + (name -> value); this }
}
