package com.it21learning.common

import scala.reflect.runtime.universe._
import com.typesafe.config.Config
import scala.reflect.runtime.universe
import com.it21learning.common.logging.Loggable
import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer

/**
 * The base class for initializing annotated members/fields by configuration, and key-value pairs.
 */
trait PropertyInitializer extends Loggable with Serializable {
  /**
    * To set values, which are from the config, of the members annotated by ConfigKey
    * @param config - contains the values by config-keys
    */
  def init(config: Config): Seq[PropertyUse] = PropertyInitializer.init(config, this)

  /**
   * To set values, which are from the config, of the members annotated by ConfigKey
   * @param config - contains the values by config-keys
   * @param owner - the target object
   */
  def init(config: Config, owner: AnyRef = this): Seq[PropertyUse] = PropertyInitializer.init(config, owner)

  /**
   * To set values, which are from the config, of the members annotated by ConfigKey
   *
   * @param properties - contains the values by property-keys
   * @return - the properties (with their values) that are not applied.
   */
  def init(properties: Seq[(String, Any)]): Seq[PropertyUse] = PropertyInitializer.init(properties, this)

  /**
   * To set values, which are from the config, of the members annotated by ConfigKey
   *
   * @param properties - contains the values by property-keys
   * @param owner - the target object
   * @return - the properties (with their values) that are not applied.
   */
  def init(properties: Seq[(String, Any)], owner: AnyRef): Seq[PropertyUse] = PropertyInitializer.init(properties, owner)
}

object PropertyInitializer extends ValueOperator {
  /**
   * To set values, which are from the config, of the members annotated by ConfigKey
   * @param config - contains the values by config-keys
   * @param owner - the owning object of which properties will be initialized
   */
  def init(config: Config, owner: AnyRef): Seq[PropertyUse] = {
    val appliedProperties = new ArrayBuffer[PropertyUse]()
    AnnotationFinder.findMembersAnnotation[PropertyKey](owner).foreach {
      case (m: TermSymbol, a: PropertyKey) =>
        val (path, kvMap) = if (a.path.endsWith(".*")) (a.path.stripSuffix(".*"), true) else (a.path, false)
        if (config.hasPath(path)) {
          val field = universe.runtimeMirror(owner.getClass.getClassLoader).reflect(owner).reflectField(m.asTerm)
          var value = if (kvMap) {
            config.getObject(path).entrySet().asScala.map(e => (e.getKey, e.getValue.unwrapped())).toMap
          } else {
            config.getAnyRef(path)
          }
          if (field.symbol.typeSignature.resultType.toString matches "Option\\[.+\\]") {
            value = Option[AnyRef](value)
          }
          field.set(value)
          appliedProperties.append(PropertyUse(a.path, Some(value), applied = true))
        } else if (a.required) {
          appliedProperties.append(PropertyUse(a.path, None, applied = false))
        }
      case _ =>
    }
    appliedProperties
  }


  /**
   * To set values, which are from the config, of the members annotated by ConfigKey
   *
   * @param properties - contains the values by property-keys
   * @owner - the owning object of which properties will be initialized.
   */
  def init(properties: Seq[(String, Any)], owner: AnyRef): Seq[PropertyUse] = {
    val appliedProperties = new ArrayBuffer[PropertyUse]()
    AnnotationFinder.findMembersAnnotation[PropertyKey](owner).foreach {
      case (m: TermSymbol, a: PropertyKey) =>
        val (path, kvMap) = if (a.path.endsWith("*")) (a.path.stripSuffix("*"), true) else (a.path, false)
        if (kvMap && properties.exists(_._1.startsWith(path))) {
          val value = properties.filter(_._1.startsWith(path)).map { case (k, v) => (k.replaceAll(path, ""), v) }.toMap
          setValue(m, value, owner)
          appliedProperties.append(PropertyUse(a.path, Some(value), applied = true))
        }
        else if (properties.exists(_._1.equals(path))) {
          val value = properties.find(p => p._1.equals(path)).map(p => p._2).head.asInstanceOf[AnyRef]
          setValue(m, value, owner)
          appliedProperties.append(PropertyUse(a.path, Some(value), applied = true))
        } else if (a.required) {
          appliedProperties.append(PropertyUse(a.path, None, applied = false))
        }
      case _ =>
    }
    appliedProperties
  }
}