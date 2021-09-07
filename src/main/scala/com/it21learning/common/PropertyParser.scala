package com.it21learning.common

import scala.xml.{Elem, NodeSeq}
import scala.collection.mutable.{Map => MMap}

/**
 * Parse properties from xml definition
 */
trait PropertyParser extends Serializable {
  /**
   * Parse the actor properties
   * @param definition
   * @param excludeProperties
   * @return
   */
  def parse(definition: NodeSeq, excludeProperties: Seq[String]): Map[String, String] = {
    val properties = MMap[String, String]()
    parse(excludeProperties match {
      case Seq(_, _ @ _*) => (definition \ "property").filter(d => !excludeProperties.contains((d \ "@name").text))
      case _ => definition \ "property"
    })(properties)
    properties.toMap
  }

  //parse a sub properties
  private def parse(propNodes: Seq[NodeSeq], xpath: Option[String] = None)(properties: MMap[String, String]): Unit = {
    propNodes.foreach(prop => {
      val path = xpath match {
        case Some(v) => s"$v.${(prop \ "@name").text}"
        case _ => (prop \ "@name").text
      }
      prop \ "definition" match {
        case c if c.nonEmpty => parse(c, Some(path))(properties)
        case _ => properties.put(path, if (prop.asInstanceOf[Elem].child.size > 1) prop.toString() else prop.text)
      }
    })
  }
}
