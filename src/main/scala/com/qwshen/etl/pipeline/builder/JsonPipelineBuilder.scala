package com.qwshen.etl.pipeline.builder

import com.qwshen.common.io.FileChannel
import com.qwshen.common.logging.Loggable
import com.qwshen.etl.common.{Actor, UdfRegister, VariableSetter}
import com.qwshen.etl.configuration.ConfigurationManager
import com.qwshen.etl.pipeline.definition._
import com.typesafe.config.Config
import org.apache.spark.sql.SparkSession
import scala.collection.breakOut
import com.fasterxml.jackson.core.{JsonParser, JsonToken}
import com.fasterxml.jackson.databind.json.JsonMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import scala.annotation.tailrec
import scala.collection.mutable.{Map => mMap}
import scala.collection.mutable.{ArrayBuffer => mArray}
import scala.util.{Failure, Success, Try}
import scala.util.control.Breaks._

/**
 * Build a pipeline from a yaml file
 */
class JsonPipelineBuilder() extends PipelineBuilder with Loggable {
  /**
   * Build an etl-pipeline from the Json definition
   *
   * @param config
   * @return
   */
  def build(definition: String)(implicit config: Config, session: SparkSession): Option[Pipeline] = for (
    properties <- {
      this.parseFull(getJsonString(definition)).map(x => x.asInstanceOf[Map[String, Any]])
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
            case Some(kvs: Seq[Map[String, Any]] @unchecked) => parseAlias(kvs)(config)
            case Some(kvs: Map[String, Any] @unchecked) => kvs.filter(kv => kv._1.equalsIgnoreCase("include")).values.headOption.map(v => parseIncludeAlias(this.resolve(v.toString)(config))(config)).getOrElse(Map.empty[String, String])
            case _ => Map.empty[String, String]
          }
          kvs.get("udf-registration") match {
            case Some(kvs: Seq[Map[String, Any]] @unchecked) => pipeline.foreach(pl => parseUdfRegistrations(kvs, aliases, pl)(config))
            case Some(kvs: Map[String, Any] @unchecked) => pipeline.foreach(pl => kvs.filter(kv => kv._1.equalsIgnoreCase("include")).values.headOption.foreach(v => parseIncludeUdfRegistration(this.resolve(v.toString)(config), aliases, pl)(config)))
            case _ =>
          }

          kvs.get("variables") match {
            case Some(variables: Seq[Map[String, Any]] @unchecked) => for (pl <- pipeline) {
              newConfig = parseVariables(variables)(newConfig, session)
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
    pipeline.map(pl => pl.takeConfig(newConfig))
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
        def withKV = (k: String, v: String) => {
          val data = this.evaluate(this.resolve(ConfigurationManager.quote(v))(newConfig))(session)
          vars.put(k, data)
          //update the config
          newConfig = ConfigurationManager.mergeVariables(newConfig, Map(k -> data))
        }
        value.foreach(v => withKV(n, v))
        decryptionKeyString.foreach(v => withKV(s"${n}_decryptionKeyString", v))
        decryptionKeyFile.foreach(v => withKV(s"${n}_decryptionKeyFile", v))
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
  protected def parseAlias(kvs: Seq[Map[String, Any]])(config: Config): Map[String, String] = kvs.flatMap(kvs => {
    var n: Option[String] = None
    var t: Option[String] = None
    var af: Option[String] = None
    kvs.foreach(kv => (kv._1, kv._2) match {
      case ("name", s: String) => n = Some(s)
      case ("type", s: String) => t = Some(this.resolve(s)(config))
      case ("include", s: String) => af = Some(this.resolve(s)(config))
      case _ =>
    })
    if (af.nonEmpty) this.parseIncludeAlias(af.get)(config).map { case(k, v) => (Some(k), Some(v)) }.toSeq else Seq((n, t))
  }).filter(kv => kv._1.nonEmpty && kv._2.nonEmpty).map(kv => (kv._1.get, kv._2.get))(breakOut)

  //parse udf-registration defined in the pipeline
  protected def parseIncludeUdfRegistration(urFile: String, alias: Map[String, String], pipeline: Pipeline)(config: Config): Unit = {
    for (properties <- {
      this.parseFull(FileChannel.loadAsString(urFile)).map(x => x.asInstanceOf[Map[String, Any]])
    }) yield {
      properties.find(p => p._1.equalsIgnoreCase("udf-registration")).map(p => p._2 match {
        case kvs: Seq[Map[String, Any]] @unchecked => parseUdfRegistrations(kvs, alias, pipeline)(config)
        case _ =>
      })
    }
  }

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
        a match {
          case vs: VariableSetter =>
            val variables = vs.variables.mapValues(v => this.evaluate(this.resolve(ConfigurationManager.quote(v))(newConfig))(session))
            if (variables.nonEmpty) {
              newConfig = ConfigurationManager.mergeVariables(newConfig, variables)
            }
          case _ =>
        }
        job.addAction(Action(n, a, outputView, inputViews))
      }
    }
  }

  //parse one actor in a job
  private def parseActor(kv: Map[String, Any], aliases: Map[String, String])(config: Config, session: SparkSession): Option[Actor] = {
    var actor: Option[Actor] = None
    val meta = mArray[(String, String)]()
    kv.foreach(x => (x._1, x._2) match {
      case ("type", s: String) => actor = Some(Class.forName(aliases.getOrElse(s, s)).getDeclaredConstructor().newInstance().asInstanceOf[Actor])
      case ("properties", properties: Map[String, Any] @unchecked) => actor.foreach(a => {
        parseMap(properties, "properties", meta)
      })
      case _ =>
    })
    for (a <- actor) {
      a.init(meta.map { case (k, v) => (k.replaceAll("properties.", ""), evaluate(resolve(v)(config))(session)) }, config)(session)
    }
    actor
  }

  //parse udf-registration
  protected def parseUdfRegistrations(kvs: Seq[Map[String, Any]], aliases: Map[String, String], pipeline: Pipeline)(config: Config): Unit = for (kv <- kvs) {
    var udfPrefix: Option[String] = None
    var udfType: Option[String] = None
    var udfInclude: Option[String] = None
    kv.foreach(x => {
      (x._1, x._2) match {
        case ("prefix", s: String) => udfPrefix = Some(this.resolve(s)(config))
        case ("type", t: String) => udfType = Some(this.resolve(t)(config))
        case ("include", f: String) => udfInclude = Some(this.resolve(f)(config))
        case _ =>
      }
    })
    if (udfInclude.nonEmpty) {
      this.parseIncludeUdfRegistration(udfInclude.get, aliases, pipeline)(config)
    } else {
      for {
        typ <- udfType
      } {
        val register = Class.forName(aliases.getOrElse(typ, typ)).getConstructor().newInstance().asInstanceOf[UdfRegister]
        pipeline.addUdfRegister(UdfRegistration(udfPrefix.getOrElse(""), register))
      }
    }
  }

  //parse metrics-logging
  private def parseMetricsLogging(kv: Map[String, Any], pipeline: Pipeline)(config: Config): Unit = {
    var loggingEnabled: Boolean = true
    var loggingUri: Option[String] = None
    var loggingActions: Seq[String] = Nil
    kv.foreach(x => (x._1, x._2) match {
      case ("enabled", e: String) => loggingEnabled = resolve(e)(config).equalsIgnoreCase("true")
      case ("uri", s: String) => loggingUri = Some(resolve(s)(config))
      case ("actions", ss: Seq[String] @unchecked) => loggingActions = ss
      case _ =>
    })
    pipeline.takeMetricsLogging(MetricsLogging(loggingEnabled, loggingUri, loggingActions))
  }

  //parse debug-staging
  private def parseStagingBehavior(kv: Map[String, Any], pipeline: Pipeline)(config: Config): Unit = {
    var stagingEnabled: Boolean = true
    var stagingUri: Option[String] = None
    var stagingActions: Seq[String] = Nil
    kv.foreach(x => (x._1, x._2) match {
      case ("enabled", e: String) => stagingEnabled = resolve(e)(config).equalsIgnoreCase("true")
      case ("uri", s: String) => stagingUri = Some(resolve(s)(config))
      case ("actions", ss: Seq[String] @unchecked) => stagingActions = ss
      case _ =>
    })
    pipeline.takeStagingBehavior(StagingBehavior(stagingEnabled, stagingUri, stagingActions))
  }

  //Parse a Map element
  private def parseMap(itemsMap: Map[String, Any], path: String, meta: mArray[(String, String)]): Unit = itemsMap.foreach {
    case (k, v) => (k, v) match {
      case (s: String, kvs: Map[String, Any] @unchecked) => parseMap(kvs, combine(path, s), meta)
      case (s: String, items: Seq[Any] @unchecked) => parseSeq(items, combine(path, s), meta)
      case (s: String, any: Any) => meta.append((combine(path, s), any.toString))
    }
  }

  //Parse a sequence element
  private def parseSeq(itemsSeq: Seq[Any], path: String, meta: mArray[(String, String)]): Unit = itemsSeq.foreach {
    case kvs: Map[String, Any] @unchecked => parseMap(kvs, combine(path, "/"), meta)
    case items: Seq[Any] @unchecked => parseSeq(items, combine(path, "/"), meta)
    case any: Any => meta.append((combine(path, any.toString), any.toString))
  }

  //combine two path into one
  private def combine(s1: String, s2: String): String = if (s1.nonEmpty) s"$s1.$s2" else s2

  //parse included alias
  protected def parseIncludeAlias(aliasFile: String)(config: Config): Map[String, String] = (
    for (properties <- {
      this.parseFull(FileChannel.loadAsString(aliasFile)).map(x => x.asInstanceOf[Map[String, Any]])
    }) yield {
      properties.find(p => p._1.equalsIgnoreCase("aliases")).map(p => p._2 match {
        case kvs: Seq[Map[String, Any]] @unchecked => parseAlias(kvs)(config)
        case _ => Map.empty[String, String]
      })
    }
  ).flatten.getOrElse(Map.empty[String, String])

  //parse a job which is included in the definition
  protected def parseIncludeJob(jobFile: String, aliases: Map[String, String], pipeline: Pipeline)(config: Config, session: SparkSession): Unit = for (properties <- {
    this.parseFull(FileChannel.loadAsString(jobFile)).map(x => x.asInstanceOf[Map[String, Any]])
  }) {
    parseJob(properties, aliases, pipeline)(config, session)
  }

  //convert the input string to json string
  protected def getJsonString(str: String): String = str;

  /**
   * Parse a json document
   * @param json
   * @return
   */
  protected def parseFull(json: String): Option[Any] = Try {
    val parser = JsonMapper.builder().addModule(DefaultScalaModule).build().createParser(json)
    parser.nextToken()
    val jsTree = process(parser)
    jsTree
  } match {
    case Success(v) => Some(v)
    case Failure(e) => throw e
  }

  private def process(parser: JsonParser): Any = parser.currentToken() match {
    case JsonToken.START_OBJECT =>
      val jsObject = mMap.empty[String, Any]
      parser.nextToken()
      processObject(parser, jsObject)
      jsObject.toMap
    case JsonToken.START_ARRAY =>
      val jsArray = mArray[Any]()
      parser.nextToken()
      processArray(parser, jsArray)
      jsArray
    case _ => throw new RuntimeException("Invalid Json document")
  }

  @tailrec
  private def processObject(parser: JsonParser, jsObject: mMap[String, Any]): Unit = parser.currentToken() match {
    case JsonToken.END_OBJECT =>
    case _ =>
      parser.currentToken() match {
        case JsonToken.VALUE_STRING => jsObject(parser.currentName) = parser.getValueAsString
        case JsonToken.VALUE_FALSE | JsonToken.VALUE_TRUE => jsObject(parser.currentName) = parser.getValueAsBoolean
        case JsonToken.VALUE_NUMBER_INT => jsObject(parser.currentName) = parser.getValueAsInt
        case JsonToken.VALUE_NUMBER_FLOAT => jsObject(parser.currentName) = parser.getValueAsDouble

        case JsonToken.START_OBJECT | JsonToken.START_ARRAY => jsObject(parser.currentName) = process(parser)
        case JsonToken.FIELD_NAME =>
        case _ => throw new RuntimeException("Un-processed token met in processObject.")
      }
      parser.nextToken()
      processObject(parser, jsObject)
  }

  @tailrec
  private def processArray(parser: JsonParser, jsArray: mArray[Any]): Unit = parser.currentToken() match {
    case JsonToken.END_ARRAY =>
    case _ =>
      parser.currentToken() match {
        case JsonToken.VALUE_STRING => jsArray.append(parser.getValueAsString)
        case JsonToken.VALUE_FALSE | JsonToken.VALUE_TRUE => jsArray.append(parser.getValueAsBoolean)
        case JsonToken.VALUE_NUMBER_INT => jsArray.append(parser.getValueAsInt)
        case JsonToken.VALUE_NUMBER_FLOAT => jsArray.append(parser.getValueAsDouble)
        case JsonToken.VALUE_NULL => jsArray.append(null)

        case JsonToken.START_OBJECT | JsonToken.START_ARRAY => jsArray.append(process(parser))
        case _ => throw new RuntimeException("Un-processed token met in processArray.")
      }
      parser.nextToken()
      processArray(parser, jsArray)
  }
}