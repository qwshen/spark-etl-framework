package com.qwshen.common

import scala.reflect.runtime.universe._
import scala.reflect.runtime.universe
import scala.util.Try

/**
 * Defines method for manipulating value of a member in an object through reflection
 */
trait ValueOperator extends Serializable {
  /**
   * set the value for the member
   *
   * @param member
   * @param owner
   * @return
   */
  def getValue(member: TermSymbol, owner: AnyRef): Any = {
    universe.runtimeMirror(owner.getClass.getClassLoader).reflect(owner).reflectField(member.asTerm).get
  }

  /**
   * Refine the value per member's type
   *
   * @param member
   * @param value
   * @return - the new value (from value) which matches the type of the member
   */
  def matchValue(member: TermSymbol, value: AnyRef): AnyRef = member.typeSignature.resultType.toString match {
    case ValueOperator.optionType(tpe) => tpe match {
      case "Int" => Option[AnyRef](Int.box(Try(value.toString.toInt).getOrElse(value.toString.toFloat.toInt)))
      case "Boolean" => Option[AnyRef](Boolean.box(value.toString.toBoolean))
      case "Long" => Option[AnyRef](Long.box(Try(value.toString.toLong).getOrElse(value.toString.toDouble.toLong)))
      case "Byte" => Option[AnyRef](Byte.box(Try(value.toString.toByte).getOrElse(value.toString.toFloat.toByte)))
      case "Short" => Option[AnyRef](Short.box(Try(value.toString.toShort).getOrElse(value.toString.toFloat.toShort)))
      case "Float" => Option[AnyRef](Float.box(value.toString.toFloat))
      case "Double" => Option[AnyRef](Double.box(value.toString.toDouble))
      case _ => Option[AnyRef](value)
    }
    case "Int" => Int.box(Try(value.toString.toInt).getOrElse(value.toString.toFloat.toInt))
    case "Long" => Long.box(Try(value.toString.toLong).getOrElse(value.toString.toDouble.toLong))
    case "Boolean" => Boolean.box(value.toString.toBoolean)
    case "Byte" => Byte.box(Try(value.toString.toByte).getOrElse(value.toString.toFloat.toByte))
    case "Short" => Short.box(Try(value.toString.toShort).getOrElse(value.toString.toFloat.toShort))
    case "Float" => Float.box(value.toString.toFloat)
    case "Double" => Double.box(value.toString.toDouble)
    case "Char" | "String" => value.toString
    case _ => value
  }

  /**
   * set the value for the member
   *
   * @param member
   * @param value
   * @param owner
   */
  def setValue(member: TermSymbol, value: AnyRef, owner: AnyRef): Unit = {
    val field = universe.runtimeMirror(owner.getClass.getClassLoader).reflect(owner).reflectField(member.asTerm)
    field.set(matchValue(field.symbol, value))
  }
}

object ValueOperator {
  //the regular-expression for identifying Option Type
  private val optionType = "Option\\[(.+)\\]".r
}