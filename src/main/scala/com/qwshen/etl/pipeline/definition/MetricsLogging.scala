package com.qwshen.etl.pipeline.definition

/**
 * Describes the logging behavior
 *
 * @param loggingUri
 * @param loggingActions
 */
case class MetricsLogging(loggingUri: Option[String] = None, loggingActions: Seq[String] = Nil)
