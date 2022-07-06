package com.qwshen.etl.pipeline.definition

/**
 * Describes the logging behavior
 *
 * @param stagingEnabled
 * @param stagingUri
 * @param stagingActions
 */
case class StagingBehavior(stagingEnabled: Boolean, stagingUri: Option[String] = None, stagingActions: Seq[String] = Nil)
