package com.qwshen.etl.common

import com.qwshen.common.{PropertyKey, VariableResolver}
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * Set SQL Variables
 */
private[etl] class VariableSetter extends Actor with VariableResolver {
  //the variables with key-value pairs
  @PropertyKey("variables.*", false)
  protected var _variables: Map[String, String] = Map.empty[String, String]

  /**
   * Extra Variables exposed for any down-stream actions of the current job.
   * @return - any variables defined in the current actor
   */
  def variables: Map[String, String] = this._variables

  /**
   * Run the actor
   *
   * @param ctx     - the execution context
   * @param session - the spark-session
   * @return
   */
  override def run(ctx: JobContext)(implicit session: SparkSession): Option[DataFrame] = None
}
