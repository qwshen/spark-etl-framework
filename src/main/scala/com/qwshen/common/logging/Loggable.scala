package com.qwshen.common.logging

import org.slf4j.{Logger, LoggerFactory}

trait Loggable extends Serializable {
  /**
   * the Logger
   *
   * @return
   */
  protected def logger: Logger = LoggerFactory.getLogger(this.getClass)

  /**
   * Shortcut for logging error
   * @param t
   */
  def error(t: Throwable): Unit = logger.error("%s \n %s".format(t.getMessage, t.getStackTrace))
}
