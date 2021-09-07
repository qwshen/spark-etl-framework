package com.it21learning.common

import com.typesafe.config.Config
import org.apache.spark.sql.SparkSession

import scala.collection.breakOut
import scala.xml.NodeSeq

/**
 * The base property component
 */
class PropertyComponent extends PropertyInitializer with PropertyParser with PropertyValidater with Serializable {
  //the properties of current actor
  protected val properties: scala.collection.mutable.Map[String, Any] = scala.collection.mutable.Map.empty[String, Any]

  /**
   * Get the value of a property identified by propKey
   * @param propKey
   * @tparam T
   * @return
   */
  protected def getProperty[T](propKey: String): Option[T] = this.properties.get(propKey).asInstanceOf[Option[T]]

  /**
   * Get properties with the key starts with propPrefix
   * @param propPrefix
   * @return
   */
  protected def getProperties(propPrefix: String): Map[String, Any] = this.properties.filter { case(k, v) => k.startsWith(propPrefix) }.toMap

  /**
   * Initialize the actor with the definition.
   * @param definition - is defined as:
   *  <actor type="...">
   *    <property name="name1">value1</property>
   *    <property name="name2">value2</property>
   *  </actor>
   * @param config  - the configuration object
   * @param session - the spark-session object
   */
  def init(definition: NodeSeq, config: Config)(implicit session: SparkSession): Unit = {
    this.init(config)

//    //copy the properties
//    for {
//      (k, v) <- this.init(parse(definition, Nil))
//    } {
//      this.properties.put(k, v)
//    }
  }

  /**
   * Initialize the component with the properties
   *
   * @param properties
   * @param config
   */
  def init(properties: Seq[(String, String)], config: Config): Unit = {
    val propertiesUses = this.init(config) ++ this.init(properties)

    def merge(x: Option[Any], y: Option[Any]): Option[Any] = if (x.nonEmpty) x else y
    def combine(path: String, ps: Seq[PropertyUse]): PropertyUse = ps.reduce((x, y) => PropertyUse(path, merge(x.value, y.value), x.applied || y.applied))
    val props: Seq[PropertyUse] = propertiesUses.map(p => (p.path, p)).groupBy(_._1).map { case(path, ps) => combine(path, ps.map(p => p._2)) } (breakOut)

    if (props.exists(!_.applied)) {
      val keys = props.filter(!_.applied).map(p => p.path).mkString(",")
      throw new RuntimeException(s"For ${this.getClass.getCanonicalName}, the properties with keys ${keys} are required, but their value not provided.")
    }

    if (this.logger.isDebugEnabled) {
      logger.info("Properties for " + this.getClass.getCanonicalName + ":")
      props.filter(_.applied).foreach(p => logger.info(p.path + " = " + p.value))
    }
  }
}
