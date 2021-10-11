package com.qwshen.etl.pipeline.definition

import com.typesafe.config.Config
import scala.collection.mutable.ArrayBuffer

/**
 * Describe the structure of a Pipeline.
 * @param name
 */
case class Pipeline(name: String) {
  //default - true
  private var _globalViewAsLocal: Boolean = true
  /**
   * Set the flag for globalViewAsLocal
   * @param flag
   * @return
   */
  def globalViewAsLocal_=(flag: Boolean): Pipeline = { this._globalViewAsLocal = flag; this }
  /**
   * When true, all global views are down-graded to local views, so global views can be used as local views in queries.
   * @return
   */
  def globalViewAsLocal: Boolean = this._globalViewAsLocal

  //default - false
  private var _singleSparkSession: Boolean = false
  /**
   * Set the flag for singleSparkSession
   * @param flag
   * @return
   */
  def singleSparkSession_=(flag: Boolean): Pipeline = { this._singleSparkSession = flag; this }
  /**
   * When true, each job is executed under a separated sub-SparkSession.
   * @return
   */
  def singleSparkSession: Boolean = this._singleSparkSession

  //config
  private var _config: Option[Config] = None
  /**
   * The pipeline specific configuration
   * @param config
   * @return
   */
  def takeConfig(config: Config): Pipeline = { this._config = Some(config); this }
  /**
   * The config property
   * @return
   */
  def config: Option[Config] = this._config

  //job container
  private val _jobs: ArrayBuffer[Job] = new ArrayBuffer[Job]()
  /**
   * Add one job
   * @param job
   * @return
   */
  def addJob(job: Job): Pipeline = { this._jobs.append(job); this }
  /**
   * All jobs
   * @return
   */
  def jobs: Seq[Job] = this._jobs

  //udf-registration container
  private val _udfRegistrations: ArrayBuffer[UdfRegistration] = new ArrayBuffer[UdfRegistration]()
  /**
   * Add one udf-register
   * @param udfRegistration
   * @return
   */
  def addUdfRegister(udfRegistration: UdfRegistration): Pipeline = { this._udfRegistrations.append(udfRegistration); this }
  /**
   * All udf-registers
   * @return
   */
  def udfRegistrations: Seq[UdfRegistration] = this._udfRegistrations

  //logging behavior
  private var _stagingBehavior: Option[StagingBehavior] = None
  /**
   * Take the staging-behavior
   * @param stagingBehavior
   * @return
   */
  def takeStagingBehavior(stagingBehavior: StagingBehavior): Pipeline = { this._stagingBehavior = Some(stagingBehavior); this }
  /**
   * The logging behavior
   * @return
   */
  def stagingBehavior: Option[StagingBehavior] = this._stagingBehavior
}