package com.it21learning.common

import scala.reflect.runtime.universe._

/**
 * Create object of a Type with a property collection
 */
object ObjectCreator extends PropertyInitializer with ValueOperator with Serializable {
  private val mirror = runtimeMirror(getClass.getClassLoader)
  /**
   * Create an object T with parameters provided in the properties.
   * Fields (including Constructor parameters) initialization is per their annotation setting with PropertyKey
   *
   * @param properties
   * @tparam T
   * @return
   */
  def create[T <: AnyRef: TypeTag](properties: Seq[(String, AnyRef)]): Option[T] = for {
    constructor <- typeOf[T].decl(termNames.CONSTRUCTOR).asTerm.alternatives.collectFirst { case ctor: MethodSymbol if ctor.isPrimaryConstructor => ctor }
  } yield {
    val annotatedParams: Map[String, PropertyKey] = AnnotationFinder
      .findConstructorAnnotation[PropertyKey, T].map(param => (param._1.fullName, param._2))(collection.breakOut)
    val parameters: Seq[Any] = constructor.paramLists.flatMap(param => param.map {
      case member: TermSymbol => annotatedParams.get(member.fullName) match {
        case Some(a: PropertyKey) =>
          val (path, kvMap) = if (a.path.endsWith("*")) (a.path.stripSuffix("*"), true) else (a.path, false)
          if (kvMap && properties.exists(_._1.startsWith(path))) {
            properties.filter(_._1.startsWith(path)).map { case(k, v) => (k.replaceAll(String.format("^%s\\.", path), ""), v) }.toMap
          } else {
            properties.find(p => p._1.equals(path)).map(p => p._2).head match {
              case Some(value: AnyRef) => matchValue(member, value)
              case _ => throw new RuntimeException(s"The value for ${member.fullName} is not provided.")
            }
          }
        case _ => throw new RuntimeException(s"The member (${member.fullName}) is not annotated with PropertyKey.")
      }
    })
    val t = mirror.reflectClass(typeOf[T].typeSymbol.asClass).reflectConstructor(constructor).apply(parameters: _*).asInstanceOf[AnyRef]
    init(properties, t)
    t.asInstanceOf[T]
  }
}
