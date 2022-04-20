package com.qwshen.common

import com.typesafe.config.Config
import scala.collection.breakOut

/**
 * The base property component
 */
class PropertyComponent extends PropertyInitializer with PropertyValidater with Serializable {
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
      throw new RuntimeException(s"For ${this.getClass.getCanonicalName}, the properties with key(s) [$keys] required, but its(their) value(s) not provided.")
    }

    if (this.logger.isDebugEnabled) {
      logger.info("Properties for " + this.getClass.getCanonicalName + ":")
      props.filter(_.applied).foreach(p => logger.info(p.path + " = " + p.value))
    }
  }
}
