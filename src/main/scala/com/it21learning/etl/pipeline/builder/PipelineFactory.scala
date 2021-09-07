package com.it21learning.etl.pipeline.builder

import com.it21learning.etl.pipeline.definition.Pipeline
import com.typesafe.config.Config
import org.apache.spark.sql.SparkSession

object PipelineFactory {
  /**
   * Build a Pipeline from a yaml definition
   * @param definition
   * @param config
   * @return
   */
  def fromYaml(definition: String)(implicit config: Config, session: SparkSession): Option[Pipeline] = new YamlPipelineBuilder().build(definition)

  /**
   * Build a Pipeline from a json definition
   * @param definition
   * @param config
   * @return
   */
  def fromJson(definition: String)(implicit config: Config, session: SparkSession): Option[Pipeline] = new JsonPipelineBuilder().build(definition)

  /**
   * Build a Pipeline from a xml definition
   * @param definition
   * @param config
   * @return
   */
  def fromXml(definition: String)(implicit config: Config, session: SparkSession): Option[Pipeline] = new XmlPipelineBuilder().build(definition)
}
