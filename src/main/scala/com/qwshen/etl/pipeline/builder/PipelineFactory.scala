package com.qwshen.etl.pipeline.builder

import com.qwshen.common.io.FileChannel
import com.qwshen.etl.pipeline.definition.Pipeline
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

  /**
   * Build a pipeline from a pipeline-definition file
   * @param definitionFile
   * @param config
   * @param session
   * @return
   */
  def fromFile(definitionFile: String)(implicit config: Config, session: SparkSession): Option[Pipeline] = definitionFile.split("\\.").last.toLowerCase() match {
    case "yaml" | "yml" => this.fromYaml(FileChannel.loadAsString(definitionFile))(config, session)
    case "json" => this.fromJson(FileChannel.loadAsString(definitionFile))(config, session)
    case "xml" => this.fromXml(FileChannel.loadAsString(definitionFile))(config, session)
    case _ => throw new RuntimeException(s"The pipeline definition from [$definitionFile] is invalid.")
  }
}
