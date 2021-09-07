package com.it21learning.etl.setting

import com.it21learning.etl.common.{Actor, ExecutionContext}
import com.typesafe.config.Config
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.xml.NodeSeq

/**
 * Set up spark-settings
 */
final class SparkConfSetter(private val settings: Seq[(String, String)]) extends Actor {
  def this() = this(Nil)

  //container
  private val _settings: scala.collection.mutable.Map[String, String] = scala.collection.mutable.Map[String, String]()
  //append
  settings.foreach { case(name, value) => this._settings.put(name, value) }

  /**
   * Setting up the settings
   *
   * @param session
   * @return
   */
  def run(ctx: ExecutionContext)(implicit session: SparkSession): Option[DataFrame] = { _settings.foreach { case (k, v) => session.conf.set(k, v) }; None }

  /**
   * The props should come in as the following format
   *
   *   <actor type="...">
   *     <property name="spark.executor.memory">24g</property>
   *   </actor>
   *
   * @param props
   * @param config
   */
  override def init(props: NodeSeq, config: Config)(implicit session: SparkSession): Unit = {
    super.init(config)
    //extract the settings
    (props \\ "property").foreach(prop => this._settings.put((prop \ "@name").text, prop.text))
  }

  /**
   * Add a setting
   *
   * @param name
   * @param value
   * @return
   */
  def add(name: String, value: String): SparkConfSetter = { this._settings.put(name, value); this }
}
