package com.qwshen.etl.configuration

import com.qwshen.common.io.FileChannel

import java.io.File
import com.qwshen.common.logging.Loggable
import com.qwshen.etl.pipeline.definition.StagingBehavior
import com.typesafe.config.{Config, ConfigFactory}
import scopt.OptionParser

/**
 * Parse the arguments, and combine them into the configuration from the config-file.
 */
object ArgumentParser extends Loggable {
  //to describe what are in the arguments
  private case class Configuration(pipelineDef: String, applicationConf: String,
    variables: Map[String, String] = Map.empty[String, String], stagingUri: Option[String] = None, stagingActions: Seq[String] = Nil)

  //the parser
  private val scoptParser: OptionParser[Configuration] = new OptionParser[Configuration]("spark-etl-framework") {
    head("spark-etl-framework")
    opt[String]("pipeline-def")
      .required()
      .action((x, c) => c.copy(pipelineDef = x))
      .text("The pipeline definition file")
    opt[String]("application-conf")
      .required()
      .action((x, c) => c.copy(applicationConf = x))
      .text("The application configuration file(s)")
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
      val appConfig = cfg.applicationConf.split(",")
        .map(cfgFile => ConfigFactory.parseString(FileChannel.loadAsString(cfgFile))).reduce((cfg1, cfg2) => cfg2.withFallback(cfg1)).resolve()
      val config: Config = ConfigurationManager.mergeVariables(appConfig, cfg.variables)

      //staging behavior
      val stagingBehavior: Option[StagingBehavior] = (cfg.stagingUri, cfg.stagingActions) match {
        case (Some(_), Seq(_, _ @ _*)) => Some(StagingBehavior(cfg.stagingUri, cfg.stagingActions))
        case _ => None
      }

      //arguments
      Arguments(config, cfg.pipelineDef, stagingBehavior)
    }
    case _ => throw new RuntimeException("Cannot parse the arguments or load the config-file.")
  }
}
