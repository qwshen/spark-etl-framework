package com.qwshen.etl.configuration

import com.qwshen.etl.pipeline.definition.{MetricsLogging, StagingBehavior}
import com.typesafe.config.Config

/**
 * Describe the launch arguments
 * @param config
 * @param pipelineFile
 * @param staging
 * @param metricsLogging
 * @param stagingBehavior
 * @param metricsLoggingBehavior
 */
case class Arguments(config: Config, pipelineFile: String, staging: Boolean, metricsLogging: Boolean, stagingBehavior: Option[StagingBehavior] = None, metricsLoggingBehavior: Option[MetricsLogging])
