package com.qwshen.etl.pipeline.builder

import com.qwshen.common.io.FileChannel
import com.qwshen.common.logging.Loggable
import com.qwshen.etl.configuration.ConfigurationManager
import com.qwshen.etl.pipeline.definition._
import com.qwshen.etl.common.{Actor, UdfRegister}
import com.typesafe.config.Config
import org.apache.spark.sql.SparkSession

import scala.collection.mutable.ArrayBuffer
import scala.util.{Failure, Success, Try}
import scala.xml.{Elem, XML}

/**
 * Build an etl-pipeline from xml definition
 */
final class XmlPipelineBuilder extends PipelineBuilder with Loggable {
  //the pattern of xml header
  private val headPattern: String = "<\\?xml\\s+[^\\?]*\\s+\\?>"

  /**
   * Build an etl-pipeline from the xml definition
   *
   * @param definition - the pipeline definition in xml format
   * @param config - the application configuration
   * @return
   */
  def build(definition: String)(implicit config: Config, session: SparkSession): Option[Pipeline] = {
    //the xml content without the header
    val xmlContent = removeXmlHeader(definition)
    //parse
    parseString(xmlContent)
  }

  //parse the xml string
  private def parseString(xmlString: String)(implicit config: Config, session: SparkSession): Option[Pipeline] = Try {
    //check version
    "version=\"[0-9|\\.]+\"".r.findFirstIn(xmlString).map(x => x.split("=")(1).stripPrefix("\"").stripSuffix("\"")) match {
      case Some(v) if v == "1.0.0" => parse_1_0_0(xmlString)(config, session)
      case _ => throw new RuntimeException("Cannot build the pipeline from the xml definition - missing version.")
    }
  } match {
    case Success(pipeline) => pipeline
    case Failure(t) => throw new RuntimeException("Cannot parse the pipeline from the xml definition.", t)
  }

  //parse version 1.0.0
  private def parse_1_0_0(xmlString: String)(config: Config, session: SparkSession): Option[Pipeline] = Try {
    //resolve all variables inside the xml doc.
    val docRaw = XML.loadString(xmlString)
    //etl-pipeline
    val etlPipeline = Pipeline((docRaw \\ "pipeline-def" \ "@name").text)
    etlPipeline.singleSparkSession = Try((docRaw \\ "pipeline-def" \ "settings" \ "singleSparkSession" \ "@setting").text.toBoolean).toOption.getOrElse(false)
    etlPipeline.singleSparkSession = Try((docRaw \\ "pipeline-def" \ "settings" \ "globalViewAsLocal" \ "@setting").text.toBoolean).toOption.getOrElse(false)

    //alias
    val alias: Map[String, String] = this.parseAliases((docRaw \\ "pipeline-def" \ "aliases").headOption)(config)
    //user-defined-functions
    this.parseUdfRegistration((docRaw \\ "pipeline-def" \ "udf-registration").headOption, alias)(config).foreach(ur => etlPipeline.addUdfRegister(ur))

    //register UDFs
    UdfRegistration.setup(etlPipeline.udfRegistrations)(session)
    //merge the variables defined in thE etl-pipeline into the config object
    val newConfig: Config = mergeVariables(xmlString)(config, session)
    //set the config for the pipeline
    etlPipeline.takeConfig(newConfig)

    //resolve all variables inside the xml doc.
    val doc = XML.loadString(resolve(xmlString)(newConfig))
    //jobs
    (doc \\ "pipeline-def" \ "job").foreach(j => {
      val jobFile =(j \ "@include").text
      val jd = if (jobFile.nonEmpty) XML.loadString(resolve(FileChannel.loadAsString(jobFile))(newConfig)) else j

      val job = Job((jd \ "@name").text)
      var jobConfig = newConfig
      //actions
      (jd \ "action").foreach(a => {
        val name = (a \ "@name").text

        //actor
        val typ = (a \ "actor" \ "@type").text
        val properties = new ArrayBuffer[(String, String)]()
        //instantiate
        val actor = Class.forName(alias.getOrElse(typ, typ)).getDeclaredConstructor().newInstance().asInstanceOf[Actor]
        (a \ "actor" \ "properties").foreach(propNode => parseProperty(propNode, "", properties))
        actor.init(properties.map { case(k, v) => (k.replaceAll("properties.", ""), evaluate(v)(session)) }, jobConfig)(session)

        //input views
        val inputViews: Seq[String] = (a \ "input-views" \ "view").map(v => (v \ "@name").text)
        //output view
        val outputView: Option[View] = (a \ "output-view").headOption.map(v => View((v \ "@name").text, Try((v \ "@global").text.toBoolean).toOption.getOrElse(false)))

        //add action
        val variables = actor.extraVariables.mapValues(v => this.evaluate(this.resolve(ConfigurationManager.quote(v))(jobConfig))(session))
        if (variables.nonEmpty) {
          jobConfig = ConfigurationManager.mergeVariables(jobConfig, variables)
        }
        job.addAction(Action(name, actor, outputView, inputViews))
      })

      //add job
      etlPipeline.addJob(job)
    })

    //metrics logging
    (doc \\ "metrics-logging").headOption.map(l => {
      var loggingEnabled: Boolean = (l \ "@enabled").headOption.exists(e => e.text.equalsIgnoreCase("true"))
      val loggingUri: Option[String] = (l \ "uri").headOption.map(s => s.text)
      val loggingActions: Seq[String] = (l \ "actions" \ "action").map(a => (a \ "@name").text)
      //add the metrics logging
      etlPipeline.takeMetricsLogging(MetricsLogging(loggingEnabled, loggingUri, loggingActions))
    })

    //logging behavior
    (doc \\ "debug-staging").headOption.map(l => {
      var stagingEnabled: Boolean = (l \ "@enabled").headOption.exists(e => e.text.equalsIgnoreCase("true"))
      val stagingUri: Option[String] = (l \ "uri").headOption.map(s => s.text)
      val stagingActions: Seq[String] = (l \ "actions" \ "action").map(a => (a \ "@name").text)
      //add the logging behavior
      etlPipeline.takeStagingBehavior(StagingBehavior(stagingEnabled, stagingUri, stagingActions))
    })

    etlPipeline
  } match {
    case Success(pipeline) => Some(pipeline)
    case Failure(exception) => throw new RuntimeException("Cannot parse the pipeline definition.", exception)
  }

  //parse aliases
  private def parseAliases(aliasNode: Option[scala.xml.Node])(config: Config): Map[String, String] = {
    val includeAliases = aliasNode.flatMap(nd => (nd \ "@include").headOption).map(i => this.resolve(FileChannel.loadAsString(i.text))(config))
      .map(s => XML.loadString(this.removeXmlHeader(s)))
      .map(doc => (doc \\ "aliases").headOption).map(an => parseAliases(an)(config)).getOrElse(Map.empty[String, String])
    val aliases = aliasNode.map(an => (an \ "alias").map(a => ((a \ "@name").text, (a \ "@type").text)).toMap).getOrElse(Map.empty[String, String])
    includeAliases.++(aliases)
  }

  //parse udf-registrations
  private def parseUdfRegistration(urNode: Option[scala.xml.Node], alias: Map[String, String])(config: Config): Seq[UdfRegistration] = {
    val includeURs = urNode.flatMap(urn => (urn \ "@include").headOption).map(i => this.resolve(FileChannel.loadAsString(i.text))(config))
      .map(s => XML.loadString(this.removeXmlHeader(s)))
      .map(doc => (doc \\ "udf-registration").headOption).map(ur => parseUdfRegistration(ur, alias)(config)).getOrElse(Nil)
    val urs = urNode.map(urn => (urn \ "register").map(r => {
      val typ = (r \ "@type").text
      UdfRegistration((r \ "@prefix").text, Class.forName(alias.getOrElse(typ, typ)).getConstructor().newInstance().asInstanceOf[UdfRegister])
    })).getOrElse(Nil)
    includeURs.++(urs)
  }

  //parse the properties of an Actor
  private def parseProperty(propNode: scala.xml.Node, path: String, props: ArrayBuffer[(String, String)]): Unit = if (!propNode.isAtom) {
    val newPath = if (path.nonEmpty) s"$path.${propNode.label}" else propNode.label
    propNode.attributes.asAttrMap.foreach(attr => props.append((s"$newPath.${attr._1}", attr._2)))

    val children = propNode.child.filter(c => c.isInstanceOf[scala.xml.Elem])
    if (children.nonEmpty) {
      children.foreach(c => parseProperty(c, newPath, props))
    } else {
      props.append((newPath, propNode.text))
    }
  }

  //merge the variables defined
  private def mergeVariables(xmlString: String)(implicit config: Config, session: SparkSession): Config = Try {
    var newConfig = config
    val kvs: scala.collection.mutable.Map[String, String] = scala.collection.mutable.Map[String, String]()
    val variables = (XML.loadString(xmlString) \\ "pipeline-def" \ "variables" \ "variable").map(v => {
      val name = (v \ "@name").text
      val value = this.evaluate(this.resolve(ConfigurationManager.quote((v \ "@value").text))(newConfig))
      kvs.put(name, value)
      newConfig = ConfigurationManager.mergeVariables(newConfig, Map(name -> value))

      val keyString = (v \ "@decryptionKeyString").headOption.map(n => n.text)
      val keyFile = (v \ "@decryptionKeyFile").headOption.map(n => n.text)
      (keyString, keyFile) match {
        case (Some(ks), _) =>
          kvs.put(s"${name}_decryptionKeyString", ks)
          newConfig = ConfigurationManager.mergeVariables(newConfig, Map(s"${name}_decryptionKeyString" -> this.evaluate(this.resolve(ConfigurationManager.quote(ks)))))
        case (_, Some(kf)) =>
          kvs.put(s"${name}_decryptionKeyFile", kf)
          newConfig = ConfigurationManager.mergeVariables(newConfig, Map(s"${name}_decryptionKeyFile" -> this.evaluate(this.resolve(ConfigurationManager.quote(kf)))))
        case _ =>
      }
      (name, value, keyString, keyFile)
    }).filter(x => x._3.isDefined || x._4.isDefined)

    val decryptVariables = variables.map {
      case(name: String, _, keyString: Option[String], keyFile: Option[String]) =>
        val decryptionKey = (keyString, keyFile) match {
          case (Some(_), _) => newConfig.getString(s"${name}_decryptionKeyString")
          case (_, Some(_)) => FileChannel.loadAsString(newConfig.getString(s"${name}_decryptionKeyFile"))
          case _ => throw new RuntimeException("System error.")
        }
        (name, ConfigurationManager.decrypt(newConfig.getString(name), decryptionKey))
    }
    if (decryptVariables.nonEmpty) ConfigurationManager.mergeVariables(newConfig, decryptVariables) else newConfig
  } match {
    case Success(r) => r
    case Failure(t) => throw new RuntimeException("Parsing variables failed. Please check all variables are well defined.", t)
  }

  //remove the xml header if any
  private def removeXmlHeader(xmlString: String): String = this.headPattern.r.findFirstIn(xmlString) match {
    //remove the xml header
    case Some(head) => xmlString.replace(head, "")
    case _ => xmlString
  }
}
