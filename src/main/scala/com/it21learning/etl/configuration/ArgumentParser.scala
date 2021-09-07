package com.it21learning.etl.configuration

import java.io.File
import com.it21learning.common.logging.Loggable
import com.it21learning.etl.pipeline.definition.StagingBehavior
import com.typesafe.config.{Config, ConfigFactory}
import scopt.OptionParser

/**
 * Parse the arguments, and combine them into the configuration from the config-file.
 */
object ArgumentParser extends Loggable {
  //to describe what are in the arguments
  private case class Configuration(appConf: String, topologyClass: String, definitionFile: Option[String] = None,
    variables: Map[String, String] = Map.empty[String, String], stagingUri: Option[String] = None, stagingActions: Seq[String] = Nil,
    appName: Option[String] = None
  )

  //the parser
  private val scoptParser: OptionParser[Configuration] = new OptionParser[Configuration]("spark-etl-framework") {
    head("spark-etl-framework")
    opt[String]("app-conf")
      .required()
      .action((x, c) => c.copy(appConf = x))
      .text("The application config-file")
    opt[String]("topology-class")
      .required()
      .action((x, c) => c.copy(topologyClass = x))
      .text("The Topology class")
    opt[String]("definition")
      .optional()
      .action((x, c) => c.copy(definitionFile = Option(x)))
      .text("The topology definition file")
    opt[String]("var")
      .optional()
      .unbounded()
      .valueName("k=v")
      .action((x, c) => x.split("=") match { case Array(k, v) => c.copy(variables = c.variables + (k -> v)) })
      .text("The variable")
    opt[Map[String, String]]("vars")
      .optional()
      .unbounded()
      .valueName("k1=v1,k2=v2...")
      .action((x, c) => c.copy(variables = c.variables ++ x))
      .text("The variables")
    opt[String]("staging-uri")
      .optional()
      .action((x, c) => c.copy(stagingUri = Option(x)))
      .text("The staging-uri")
    opt[String]("staging-actions")
      .optional()
      .unbounded()
      .action((x, c) => c.copy(stagingActions = c.stagingActions ++ x.split(",")))
      .text("The staging actions")
    opt[String]("app-name")
      .optional()
      .action((x, c) => c.copy(appName = Option(x)))
      .text("The application name")
  }

  /**
   * Parse the arguments
   *
   * @param args
   * @return
   */
  def parse(args: Array[String]): Arguments = scoptParser.parse(args, Configuration("", "")) match {
    case Some(cfg) => {
      //merge the variables into the config object
      val config: Config = ConfigurationManager.mergeVariables(ConfigFactory.parseFile(new File(cfg.appConf)), cfg.variables)

      //staging behavior
      val stagingBehavior: Option[StagingBehavior] = if (cfg.stagingUri.nonEmpty || cfg.stagingActions.nonEmpty) {
        Some(StagingBehavior(cfg.stagingUri, cfg.stagingActions))
      } else None

      //arguments
      Arguments(config, cfg.topologyClass, cfg.definitionFile, stagingBehavior, cfg.appName)
    }
    case _ => throw new RuntimeException("Cannot parse the arguments or load the config-file.")
  }
}
