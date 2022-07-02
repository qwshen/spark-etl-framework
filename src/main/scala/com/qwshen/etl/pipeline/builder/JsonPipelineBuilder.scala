package com.qwshen.etl.pipeline.builder

import com.qwshen.common.io.FileChannel
import com.qwshen.common.logging.Loggable
import com.qwshen.etl.common.{Actor, UdfRegister}
import com.qwshen.etl.configuration.ConfigurationManager
import com.qwshen.etl.pipeline.definition._
import com.typesafe.config.Config
import org.apache.spark.sql.SparkSession
import scala.collection.breakOut
import scala.util.parsing.json.JSON
import scala.collection.mutable.{Map => mMap}
import scala.collection.mutable.{ArrayBuffer => sMap}
import scala.util.Try
import scala.util.control.Breaks._

/**
 * Build a pipeline from a yaml file
 */
class JsonPipelineBuilder extends PipelineBuilder with Loggable {
  /**
   * Build an etl-pipeline from the Json definition
   *
   * @param config
   * @return
   */
  def build(definition: String)(implicit config: Config, session: SparkSession): Option[Pipeline] = for (
    properties <- {
      JSON.parseFull(getJsonString(definition)).map(x => x.asInstanceOf[Map[String, Any]])
    }) yield {
    var pipeline: Option[Pipeline] = None
    breakable(
      properties.foreach {
        case (k, v) => (k, v) match {
          case ("pipeline-def", kvs: Map[String, Any] @unchecked) => kvs.foreach {
            case (jk, jv) => (jk, jv) match {
              case ("version", v: String) => v match {
                case "1.0.0" =>
                  pipeline = parse_1_0_0(properties, config)(session)
                  break
                case _ => throw new RuntimeException("Invalid version of the pipeline definition found.")
              }
              case _ =>
            }
          }
          case _ =>
        }
        case _ =>
      }
    )
    pipeline.getOrElse(throw new RuntimeException("Failed to parse the pipeline. Please check the definition."))
  }

  //parse version 1.0.0 definition
  private def parse_1_0_0(properties: Map[String, Any], config: Config)(session: SparkSession): Option[Pipeline] = {
    var newConfig: Config = config
    var pipeline: Option[Pipeline] = None
    properties.foreach {
      case (k, v) => (k, v) match {
        case ("pipeline-def", kvs: Map[String, Any] @unchecked) =>
          kvs.get("name") match {
            case Some(s: String) => pipeline = Some(Pipeline(s))
            case _ => throw new RuntimeException("The name of the pipeline is not defined.")
          }

          kvs.get("settings") match {
            case Some(kvs: Map[String, Any] @unchecked) => pipeline.foreach(pl => parseSettings(kvs, pl)(newConfig))
            case _ =>
          }
          val aliases = kvs.get("aliases") match {
            case Some(kvs: Seq[Map[String, Any]] @unchecked) => parseAlias(kvs)
            case _ => Map.empty[String, String]
          }
          kvs.get("udf-registration") match {
            case Some(kvs: Seq[Map[String, Any]] @unchecked) => pipeline.foreach(pl => parseUdfRegistrations(kvs, aliases, pl))
            case _ =>
          }

          kvs.get("variables") match {
            case Some(variables: Seq[Map[String, Any]] @unchecked) => for (pl <- pipeline) {
              UdfRegistration.setup(pl.udfRegistrations)(session)
              newConfig = parseVariables(variables)(newConfig, session)
              pl.takeConfig(newConfig)
            }
            case _ =>
          }

          kvs.get("jobs") match {
            case Some(jobs: Seq[Map[String, Any]] @unchecked) => pipeline.foreach(pl => parseJobs(jobs, aliases, pl)(newConfig, session))
            case _ => throw new RuntimeException("The jobs are not defined in the pipeline.")
          }

          kvs.get("metrics-logging") match {
            case Some(kvs: Map[String, Any] @unchecked) => pipeline.foreach(pl => parseMetricsLogging(kvs, pl)(newConfig))
            case _ =>
          }

          kvs.get("debug-staging") match {
            case Some(kvs: Map[String, Any] @unchecked) => pipeline.foreach(pl => parseStagingBehavior(kvs, pl)(newConfig))
            case _ =>
          }
        case _ =>
      }
      case _ =>
    }
    pipeline
  }

  //parse variables
  private def parseVariables(variables: Seq[Map[String, Any]])(config: Config, session: SparkSession): Config = {
    var newConfig = config
    val vars: mMap[String, String] = mMap[String, String]()
    variables.foreach(variable => {
      var name: Option[String] = None
      var value: Option[String] = None
      var decryptionKeyString: Option[String] = None
      var decryptionKeyFile: Option[String] = None
      variable.foreach {
        case (k: String, v: String) => k match {
          case "name" => name = Some(v)
          case "value" => value = Some(v)
          case "decryptionKeyString" => decryptionKeyString = Some(v)
          case "decryptionKeyFile" => decryptionKeyFile = Some(v)
          case _ =>
        }
        case _ =>
      }
      for (n <- name) {
        value.foreach(v => {
          val data = this.evaluate(this.resolve(ConfigurationManager.quote(v))(newConfig))(session)
          vars.put(n, data)
          newConfig = ConfigurationManager.mergeVariables(newConfig, Map(n -> data))
        })
        decryptionKeyString.foreach(v => vars.put(s"${n}_decryptionKeyString", v))
        decryptionKeyFile.foreach(v => vars.put(s"${n}_decryptionKeyFile", v))
      }
    })

    val decryptVariables = vars.keys.filter(k => !k.endsWith("decryptionKeyString") && !k.endsWith("decryptionKeyFile")).map(k => {
      val decryptKeyString = s"${k}_decryptionKeyString"
      val decryptKeyFile = s"${k}_decryptionKeyFile"
      val v = (vars.get(decryptKeyString), vars.get(decryptKeyFile)) match {
        case (Some(_), _) => ConfigurationManager.decrypt(newConfig.getString(k), newConfig.getString(decryptKeyString))
        case (_, Some(_)) => ConfigurationManager.decrypt(newConfig.getString(k), FileChannel.loadAsString(newConfig.getString(decryptKeyFile)))
        case (_, _) => newConfig.getString(k)
      }
      (k, ConfigurationManager.quote(v))
    })
    if (decryptVariables.nonEmpty) ConfigurationManager.mergeVariables(newConfig, decryptVariables.toMap) else newConfig
  }

  //parse the global settings
  private def parseSettings(kvs: Map[String, Any], pipeline: Pipeline)(config: Config): Unit = {
    kvs.foreach(kv => (kv._1, kv._2) match {
      case ("globalViewAsLocal", v: String) => pipeline.globalViewAsLocal = Try(resolve(v)(config).toBoolean).getOrElse(true)
      case ("globalViewAsLocal", b: Boolean) => pipeline.globalViewAsLocal = b
      case ("singleSparkSession", v: String) => pipeline.singleSparkSession = Try(resolve(v)(config).toBoolean).getOrElse(false)
      case ("singleSparkSession", b: Boolean) => pipeline.singleSparkSession = b
      case _ =>
    })
  }

  //parse alias defined in the pipeline
  private def parseAlias(kvs: Seq[Map[String, Any]]): Map[String, String] = kvs.map(kvs => {
    var n: Option[String] = None
    var t: Option[String] = None
    kvs.foreach(kv => (kv._1, kv._2) match {
      case ("name", s: String) => n = Some(s)
      case ("type", s: String) => t = Some(s)
      case _ =>
    })
    (n, t)
  }).filter(kv => kv._1.nonEmpty && kv._2.nonEmpty).map(kv => (kv._1.get, kv._2.get))(breakOut)

  //parse all jobs defined in the pipeline
  private def parseJobs(kvs: Seq[Map[String, Any]], aliases: Map[String, String], pipeline: Pipeline)(config: Config, session: SparkSession): Unit = for (kv <- kvs) {
    parseJob(kv, aliases, pipeline)(config, session)
  }

  //parse one job
  protected def parseJob(kv: Map[String, Any], aliases: Map[String, String], pipeline: Pipeline)(config: Config, session: SparkSession): Unit = {
    var job: Option[Job] = None
    kv.get("include") match {
      case Some(f: String) => parseIncludeJob(resolve(f)(config), aliases, pipeline)(config, session)
      case _ =>
        kv.get("name") match {
          case Some(s: String) => job = Some(Job(resolve(s)(config)))
          case _ => throw new RuntimeException("The name of a job is not defined.")
        }
        kv.get("actions") match {
          case Some(actions: Seq[Map[String, Any]] @unchecked) => job.foreach(j => parseActions(actions, j, aliases)(config, session))
          case _ =>
        }
    }
    job.foreach(x => pipeline.addJob(x))
  }

  //parse the actions of a job
  private def parseActions(kvs: Seq[Map[String, Any]], job: Job, aliases: Map[String, String])(config: Config, session: SparkSession): Unit = {
    var newConfig = config;
    for (kv <- kvs) {
      var name: Option[String] = None
      var actor: Option[Actor] = None
      var outputView: Option[View] = None
      var inputViews: Seq[String] = Nil
      kv.foreach(x => (x._1, x._2) match {
        case ("name", s: String) => name = Some(resolve(s)(newConfig))
        case ("actor", a: Map[String, Any] @unchecked) => actor = parseActor(a, aliases)(newConfig, session)
        case ("output-view", v: Map[String, Any] @unchecked) =>
          var vName: Option[String] = None
          var vGlobal: Boolean = false
          v.foreach(x => {
            (x._1, x._2) match {
              case ("name", s: String) => vName = Some(resolve(s)(newConfig))
              case ("global", s: String) => vGlobal = Try(resolve(s)(newConfig).toBoolean).getOrElse(false)
              case ("global", b: Boolean) => vGlobal = b
              case _ =>
            }
          })
          outputView = vName.map(vn => View(vn, vGlobal))
        case ("input-views", vs: Seq[String] @unchecked) => inputViews = vs.map(s => resolve(s)(newConfig))
        case _ =>
      })
      for {
        n <- name
        a <- actor
      } {
        val variables = a.extraVariables.mapValues(v => this.evaluate(this.resolve(ConfigurationManager.quote(v))(newConfig))(session))
        if (variables.nonEmpty) {
          newConfig = ConfigurationManager.mergeVariables(newConfig, variables)
        }
        job.addAction(Action(n, a, outputView, inputViews))
      }
    }
  }

  //parse one actor in a job
  private def parseActor(kv: Map[String, Any], aliases: Map[String, String])(config: Config, session: SparkSession): Option[Actor] = {
    var actor: Option[Actor] = None
    val meta = sMap[(String, String)]()
    kv.foreach(x => (x._1, x._2) match {
      case ("type", s: String) => actor = Some(Class.forName(aliases.getOrElse(s, s)).getDeclaredConstructor().newInstance().asInstanceOf[Actor])
      case ("properties", properties: Map[String, Any] @unchecked) => actor.foreach(a => {
        parseMap(properties, "properties", meta)
      })
      case _ =>
    })
    for (a <- actor) {
      a.init(meta.map { case (k, v) => (k.replaceAll("properties.", ""), resolve(v)(config)) }, config)(session)
    }
    actor
  }

  //parse udf-registration
  private def parseUdfRegistrations(kvs: Seq[Map[String, Any]], aliases: Map[String, String], pipeline: Pipeline): Unit = for (kv <- kvs) {
    var udfPrefix: Option[String] = None
    var udfType: Option[String] = None
    kv.foreach(x => {
      (x._1, x._2) match {
        case ("prefix", s: String) => udfPrefix = Some(s)
        case ("type", t: String) => udfType = Some(t)
        case _ =>
      }
    })
    for {
      prefix <- udfPrefix
      typ <- udfType
    } {
      val register = Class.forName(aliases.getOrElse(typ, typ)).getConstructor().newInstance().asInstanceOf[UdfRegister]
      pipeline.addUdfRegister(UdfRegistration(prefix, register))
    }
  }

  //parse metrics-logging
  private def parseMetricsLogging(kv: Map[String, Any], pipeline: Pipeline)(config: Config): Unit = {
    var loggingUri: Option[String] = None
    var loggingActions: Seq[String] = Nil
    kv.foreach(x => (x._1, x._2) match {
      case ("uri", s: String) => loggingUri = Some(resolve(s)(config))
      case ("actions", ss: Seq[String] @unchecked) => loggingActions = ss
      case _ =>
    })
    pipeline.takeMetricsLogging(MetricsLogging(loggingUri, loggingActions))
  }

  //parse debug-staging
  private def parseStagingBehavior(kv: Map[String, Any], pipeline: Pipeline)(config: Config): Unit = {
    var stagingUri: Option[String] = None
    var stagingActions: Seq[String] = Nil
    kv.foreach(x => (x._1, x._2) match {
      case ("uri", s: String) => stagingUri = Some(resolve(s)(config))
      case ("actions", ss: Seq[String] @unchecked) => stagingActions = ss
      case _ =>
    })
    pipeline.takeStagingBehavior(StagingBehavior(stagingUri, stagingActions))
  }

  //Parse a Map element
  private def parseMap(itemsMap: Map[String, Any], path: String, meta: sMap[(String, String)]): Unit = itemsMap.foreach {
    case (k, v) => (k, v) match {
      case (s: String, kvs: Map[String, Any] @unchecked) => parseMap(kvs, combine(path, s), meta)
      case (s: String, items: Seq[Any] @unchecked) => parseSeq(items, combine(path, s), meta)
      case (s: String, any: Any) => meta.append((combine(path, s), any.toString))
    }
  }

  //Parse a sequence element
  private def parseSeq(itemsSeq: Seq[Any], path: String, meta: sMap[(String, String)]): Unit = itemsSeq.foreach {
    case kvs: Map[String, Any] @unchecked => parseMap(kvs, combine(path, "/"), meta)
    case items: Seq[Any] @unchecked => parseSeq(items, combine(path, "/"), meta)
    case any: Any => meta.append((combine(path, any.toString), any.toString))
  }

  //combine two path into one
  private def combine(s1: String, s2: String): String = if (s1.nonEmpty) s"$s1.$s2" else s2

  //parse a job which is included in the definition
  protected def parseIncludeJob(jobFile: String, aliases: Map[String, String], pipeline: Pipeline)(config: Config, session: SparkSession): Unit = {
    for (properties <- {
      JSON.parseFull(FileChannel.loadAsString(jobFile)).map(x => x.asInstanceOf[Map[String, Any]])
    }) {
      parseJob(properties, aliases, pipeline)(config, session)
    }
  }

  //convert the input string to json string
  protected def getJsonString(str: String): String = str;
}


