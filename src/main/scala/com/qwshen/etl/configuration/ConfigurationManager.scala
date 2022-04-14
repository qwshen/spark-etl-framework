package com.qwshen.etl.configuration

import com.qwshen.common.security.SecurityChannel
import com.typesafe.config.{Config, ConfigFactory}
import com.qwshen.common.VariableResolver

/**
 * Utility class to manipulate configuration
 */
object ConfigurationManager {
  /**
   * Decrypt the value with the key
   *
   * @param value
   * @param keyString
   * @return
   */
  def decrypt(value: String, keyString: String): String = new SecurityChannel(keyString).decrypt(value)

  /**:
   * if the configuration value is URL, quote it.
   *
   * @param value
   * @return
   */
  def quote(value: String): String = VariableResolver.getName(value) match {
    case `value` => "[\\W]+".r.findFirstIn(value).foldLeft(value)((r, _) => "\"" + r + "\"")
    case _ => value
  }

  /**
   * Merge the variables into the config object
   *
   * @param config
   * @param variables
   * @return
   */
  def mergeVariables(config: Config, variables: Map[String, String]): Config = {
    val vs = variables.map { case(k, v) => s"$k = $v" } mkString(scala.util.Properties.lineSeparator)
    vs.headOption.map(_ => ConfigFactory.parseString(vs).withFallback(config).resolve()).getOrElse(config)
  }

  /**
   * Merge the variables into the config object
   *
   * @param config
   * @param variables
   * @return
   */
  def mergeVariables(config: Config, variables: Seq[(String, String)]): Config = mergeVariables(config, variables.toMap)
}
