package com.qwshen.etl.pipeline.definition

/**
 * Describes the logging behavior
 *
 * @param loggingEnabled
 * @param loggingUri
 * @param loggingActions
 */
case class MetricsLogging(loggingEnabled: Boolean, loggingUri: Option[String] = None, loggingActions: Seq[String] = Nil)
