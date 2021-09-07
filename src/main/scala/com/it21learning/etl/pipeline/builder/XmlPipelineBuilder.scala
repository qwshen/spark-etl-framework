package com.it21learning.etl.pipeline.builder

import com.it21learning.common.io.FileChannel
import com.it21learning.common.logging.Loggable
import com.it21learning.etl.configuration.ConfigurationManager
import com.it21learning.etl.pipeline.definition._
import com.it21learning.etl.common.{Actor, UdfRegister}
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
      case Some(v) if v == "1.0.0" => parse_1_0_0(xmlString)
      case _ => throw new RuntimeException("Cannot build the etl-pipeline from the xml definition - missing version.")
    }
  } match {
    case Success(pipeline) => pipeline
    case Failure(t) => throw new RuntimeException("Cannot parse the etl-pipeline from the xml definition.", t)
  }

  //parse version 1.0.0
  private def parse_1_0_0(xmlString: String)(implicit config: Config, session: SparkSession): Option[Pipeline] = Try {
    //merge the variables defined in thE etl-pipeline into the config object
    val newConfig: Config = mergeVariables(xmlString)
    //resolve all variables inside the xml doc.
    val doc = XML.loadString(resolve(xmlString)(newConfig))

    //etl-pipeline
    val etlPipeline = Pipeline((doc \\ "pipeline-def" \ "@name").text).takeConfig(newConfig)
    etlPipeline.singleSparkSession = Try((doc \\ "pipeline-def" \ "settings" \ "singleSparkSession" \ "@setting").text.toBoolean).toOption.getOrElse(false)
    etlPipeline.singleSparkSession = Try((doc \\ "pipeline-def" \ "settings" \ "globalViewAsLocal" \ "@setting").text.toBoolean).toOption.getOrElse(false)

    //alias
    val alias: Map[String, String] = (doc \\ "pipeline-def" \ "aliases" \ "alias").map(a => ((a \ "@name").text, (a \ "@type").text)).toMap

    //jobs
    (doc \\ "pipeline-def" \ "job").foreach(j => {
      val jobFile =(j \ "@include").text
      val jd = if (jobFile.nonEmpty) XML.loadString(resolve(FileChannel.loadAsString(jobFile))(newConfig)) else j

      val job = Job((jd \ "@name").text)
      //actions
      (jd \ "action").foreach(a => {
        val name = (a \ "@name").text

        //actor
        val typ = (a \ "actor" \ "@type").text
        val properties = new ArrayBuffer[(String, String)]()
        //instantiate
        val actor = Class.forName(alias.getOrElse(typ, typ)).getDeclaredConstructor().newInstance().asInstanceOf[Actor]
        (a \ "actor" \ "properties").foreach(propNode => parseProperty(propNode, "", properties))
        actor.init(properties.map{case(k, v) => (k.replaceAll("properties.", ""), v)}, newConfig)

        //input views
        val inputViews: Seq[String] = (a \ "input-views" \ "view").map(v => (v \ "@name").text)
        //output view
        val outputView: Option[View] = (a \ "output-view").headOption.map(v => View((v \ "@name").text, Try((v \ "@global").text.toBoolean).toOption.getOrElse(false)))

        //add action
        job.addAction(Action(name, actor, outputView, inputViews))
      })

      //add job
      etlPipeline.addJob(job)
    })

    //user-defined-functions
    (doc \\ "pipeline-def" \ "udf-registration" \ "register").foreach(r => {
      val typ = (r \ "@type").text
      val register = Class.forName(alias.getOrElse(typ, typ)).getConstructor().newInstance().asInstanceOf[UdfRegister]
      etlPipeline.addUdfRegister(UdfRegistration((r \ "@prefix").text, register))
    })

    //logging behavior
    (doc \\ "debug-staging").headOption.map(l => {
      val stagingUri: Option[String] = (l \ "uri").headOption.map(s => s.text)
      val stagingActions: Seq[String] = (l \ "actions" \ "action").map(a => (a \ "@name").text)
      //add the logging behavior
      etlPipeline.takeStagingBehavior(StagingBehavior(stagingUri, stagingActions))
    })

    etlPipeline
  } match {
    case Success(pipeline) => Some(pipeline)
    case Failure(exception) => throw new RuntimeException("Cannot parse the etl-pipeline definition.", exception)
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
  private def mergeVariables(xmlString: String)(implicit config: Config): Config = Try {
    val kvs: scala.collection.mutable.Map[String, String] = scala.collection.mutable.Map[String, String]()
    val variables = (XML.loadString(xmlString) \\ "pipeline-def" \ "variables" \ "variable").map(v => {
      val name = (v \ "@name").text
      val value = ConfigurationManager.quote((v \ "@value").text)
      kvs.put(name, value)

      val keyString = (v \ "@decryptionKeyString").headOption.map(n => n.text)
      val keyFile = (v \ "@decryptionKeyFile").headOption.map(n => n.text)
      (keyString, keyFile) match {
        case (Some(ks), _) => kvs.put(s"${name}_decryptionKeyString", ks)
        case (_, Some(kf)) => kvs.put(s"${name}_decryptionKeyFile", kf)
        case _ =>
      }
      (name, value, keyString, keyFile)
    }).filter(x => x._3.isDefined || x._4.isDefined)

    val newConfig = ConfigurationManager.mergeVariables(config, kvs.toMap)
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
