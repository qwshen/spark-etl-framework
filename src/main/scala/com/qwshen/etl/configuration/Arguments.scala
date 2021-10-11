package com.qwshen.etl.configuration

import com.qwshen.etl.pipeline.definition.StagingBehavior
import com.typesafe.config.Config

/**
 * Describe the launch arguments
 * @param config
 * @param pipelineFile
 * @param stagingBehavior
 */
case class Arguments(config: Config, pipelineFile: String, stagingBehavior: Option[StagingBehavior] = None)
