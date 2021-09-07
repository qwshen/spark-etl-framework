package com.it21learning.etl.pipeline.definition

/**
 * Describes the logging behavior
 *
 * @param stagingUri
 * @param stagingActions
 */
case class StagingBehavior(stagingUri: Option[String] = None, stagingActions: Seq[String] = Nil)
