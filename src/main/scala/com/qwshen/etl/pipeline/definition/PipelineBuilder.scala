package com.qwshen.etl.pipeline.definition

import com.qwshen.common.VariableResolver
import com.typesafe.config.Config
import org.apache.spark.sql.SparkSession

/**
 * Defines the contract for building a pipeline
 */
trait PipelineBuilder extends VariableResolver with Serializable {
  /**
   * To build a etl-pipeline
   */
  def build(definition: String)(implicit config: Config, session: SparkSession): Option[Pipeline]
}
