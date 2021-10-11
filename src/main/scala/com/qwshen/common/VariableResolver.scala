package com.qwshen.common

import com.typesafe.config.Config
import scala.util.{Success, Try}
import scala.util.matching.Regex

/**
 * To resolve varaibles with configuration
 */
trait VariableResolver {
  /**
   * Resolve a variable with configuration
   * @param s
   * @param config
   * @return
   */
  def resolve(s: String)(implicit config: Config): String = VariableResolver.varPattern.findAllIn(s).toSeq.foldLeft(s)((r, v) => {
    Try(config.getAnyRef(VariableResolver.getName(v))) match {
      case Success(cv) => r.replace(v, cv.toString)
      case _ => throw new RuntimeException(s"$v is not defined. Please check the configuration & pipeline definition.")
    }
  })
}

object VariableResolver {
  //the pattern of variables. Variables are defined as ${abc_xyz}
  lazy val varPattern: Regex = "\\$\\{([a-z|A-Z])+([a-z|A-Z|0-9|_|\\.|-])*\\}".r
  //pattern for extracting the name of the variable
  lazy val namePattern: Regex = "\\$\\{([^\\}]+)\\}".r.unanchored

  /**
   * Get the name of a variable: ${name} => name
   * @param name - the initial name of the variable
   * @return
   */
  def getName(name: String): String = {
    name match {
      case VariableResolver.namePattern(n) => n
      case _ => name
    }
  }
}