package com.qwshen.etl.common

import com.qwshen.common.PropertyComponent
import com.typesafe.config.Config
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * The common behavior of all Actors
 */
abstract class Actor extends PropertyComponent with Serializable {
  /**
   * Extra Variables exposed for any down-stream actions of the current job.
   * @return - any variables defined in the current actor
   */
  def extraVariables: Map[String, String] = Map.empty[String, String]

  /**
   * Extra Views except the input views specified in the pipeline definition that are referenced/used by current actor, such as tables in a sql-join.
   * @return - any views referenced by the current actor
   */
  def extraViews: Seq[String] = Nil

  /**
   * Initialize the actor with the properties & config
   *
   * @param properties
   * @param config
   */
  def init(properties: Seq[(String, String)], config: Config)(implicit session: SparkSession): Unit = {
    super.init(properties, config)
  }

  /**
   * Run the actor
   * @param ctx     - the execution context
   * @param session - the spark-session
   * @return
   */
  def run(ctx: JobContext)(implicit session: SparkSession): Option[DataFrame]

  /**
   * Collect metrics of current actor
   * @param df
   * @param session
   * @return
   */
  def collectMetrics(df: DataFrame): Seq[(String, String)] = Nil
}
