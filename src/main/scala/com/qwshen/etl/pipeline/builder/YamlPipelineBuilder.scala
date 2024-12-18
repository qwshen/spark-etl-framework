package com.qwshen.etl.pipeline.builder

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory
import com.qwshen.common.io.FileChannel
import com.qwshen.etl.pipeline.definition._
import com.typesafe.config.Config
import org.apache.spark.sql.SparkSession

/**
 * Build a pipeline from a yaml file
 */
final class YamlPipelineBuilder extends JsonPipelineBuilder {
  //parse a job which is included in the definition
  override protected def parseIncludeJob(jobFile: String, aliases: Map[String, String], pipeline: Pipeline)(config: Config, session: SparkSession): Unit = for (properties <- {
    this.parseFull(getJsonString(FileChannel.loadAsString(jobFile))).map(x => x.asInstanceOf[Map[String, Any]])
  }) {
    properties.foreach {
      case (k, v) => (k, v) match {
        case ("job", kvJob: Map[String, Any] @unchecked) => parseJob(kvJob, aliases, pipeline)(config, session)
        case _ => throw new RuntimeException(s"Invalid job definition from $jobFile")
      }
    }
  }

  //parse included alias
  override protected def parseIncludeAlias(aliasFile: String)(config: Config): Map[String, String] = (
    for (properties <- {
      this.parseFull(getJsonString(FileChannel.loadAsString(aliasFile))).map(x => x.asInstanceOf[Map[String, Any]])
    }) yield {
      properties.map {
        case (k, v) => (k, v) match {
          case ("aliases", kvAlias: Seq[Map[String, Any]] @unchecked) => parseAlias(kvAlias)(config)
          case _ => throw new RuntimeException(s"Invalid alias-definition from $aliasFile")
        }
      }
    }
  ).map(x => x.flatten.toMap).getOrElse(Map.empty[String, String])

  //parse included udf-registration
  override protected def parseIncludeUdfRegistration(urFile: String, alias: Map[String, String], pipeline: Pipeline)(config: Config): Unit = for (properties <- {
    this.parseFull(getJsonString(FileChannel.loadAsString(urFile))).map(x => x.asInstanceOf[Map[String, Any]])
  }) {
    properties.foreach {
      case (k, v) => (k, v) match {
        case ("udf-registration", kvAlias: Seq[Map[String, Any]] @unchecked) => parseUdfRegistrations(kvAlias, alias, pipeline)(config)
        case _ => throw new RuntimeException(s"Invalid udf-registration-definition from $urFile")
      }
    }
  }

  //convert the yaml string to json string
  override protected def getJsonString(str: String): String = {
    val data = new ObjectMapper(new YAMLFactory()).readValue(str, classOf[Any])
    new ObjectMapper().writeValueAsString(data)
  }
}