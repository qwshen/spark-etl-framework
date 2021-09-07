package com.it21learning.etl.configuration

import com.it21learning.etl.pipeline.definition.StagingBehavior
import com.typesafe.config.Config

/**
 * Describe the launch arguments
 * @param config
 * @param topologyClass
 * @param definitionFile
 * @param stagingBehavior
 * @param appName
 */
case class Arguments(config: Config,
  topologyClass: String, definitionFile: Option[String] = None, stagingBehavior: Option[StagingBehavior] = None, appName: Option[String] = None)
