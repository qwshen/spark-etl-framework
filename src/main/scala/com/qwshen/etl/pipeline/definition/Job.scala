package com.qwshen.etl.pipeline.definition

import scala.collection.mutable.ArrayBuffer

/**
 * Defines the structure of a job.
 * @param name
 */
case class Job(name: String) {
  //action container
  private val _actions: ArrayBuffer[Action] = new ArrayBuffer[Action]()
  /**
   * All actions
   *
   * @return
   */
  def actions: Seq[Action] = this._actions

  /**
   * Add an action into the job
   *
   * @param action
   * @return
   */
  def addAction(action: Action): Job = { this._actions.append(action); this }
}