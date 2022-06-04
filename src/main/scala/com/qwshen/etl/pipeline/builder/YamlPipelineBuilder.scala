package com.qwshen.etl.pipeline.builder

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory
import com.qwshen.common.io.FileChannel
import com.qwshen.etl.pipeline.definition._
import com.typesafe.config.Config
import org.apache.spark.sql.SparkSession

import scala.util.parsing.json.JSON

/**
 * Build a pipeline from a yaml file
 */
final class YamlPipelineBuilder extends JsonPipelineBuilder {
  //parse a job which is included in the definition
  override protected def parseIncludeJob(jobFile: String, aliases: Map[String, String], pipeline: Pipeline)(implicit config: Config, session: SparkSession): Unit = {
    for (properties <- {
      val data = new ObjectMapper(new YAMLFactory()).readValue(FileChannel.loadAsString(jobFile), classOf[Any])
      val jsonString = new ObjectMapper().writeValueAsString(data)
      JSON.parseFull(jsonString).map(x => x.asInstanceOf[Map[String, Any]])
    }) {
      properties.foreach {
        case (k, v) => (k, v) match {
          case ("job", kvJob: Map[String, Any] @unchecked) => parseJob(kvJob, aliases, pipeline)
          case _ => throw new RuntimeException(s"Invalid job definition from $jobFile")
        }
      }
    }
  }

  //convert the yaml string to json string
  override protected def getJsonString(str: String): String = {
    val data = new ObjectMapper(new YAMLFactory()).readValue(str, classOf[Any])
    new ObjectMapper().writeValueAsString(data)
  }
}