package com.qwshen.etl.common

import com.qwshen.common.PropertyComponent
import com.typesafe.config.Config
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * The common behavior of all Actors
 */
abstract class Actor extends PropertyComponent with Serializable {
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
