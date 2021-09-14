package com.it21learning.common

import scala.reflect.ClassTag
import scala.reflect.runtime.universe._

/**
 * Utility class for finding annotations applied on properties of an object
 */
object AnnotationFinder extends Serializable {
  /**
   * To find the annotation of type A which applied on the object (any) at member level
   *
   * @param a - the class-tag of A
   * @tparam A - the type of the annotation
   * @any -> the object on which the annotations are applied on its members.
   * @return
   */
  def findMembersAnnotation[A <: scala.annotation.Annotation: TypeTag](any: AnyRef)(implicit a: ClassTag[A]): Seq[(TermSymbol, A)] = {
    val cls = any.getClass
    val members = runtimeMirror(cls.getClassLoader).staticClass(cls.getName).selfType.members
      .collect { case s: TermSymbol => s }
      .filter(s => s.isGetter).map(s => s.accessed).filter(_.annotations.exists(a => a.tree.tpe <:< typeOf[A]))

    var constructor: Option[MethodMirror] = None
    members.flatMap(m => m.annotations.find(_.tree.tpe =:= typeOf[A]).map((m, _))).map(x => {
      if (constructor.isEmpty) {
        constructor = Some(getAnnotationConstructor(x._2))
      }
      val parameters = x._2.tree.children.tail.collect { case Literal(Constant(v: Any)) => v }
      (x._1.asTerm, constructor.get(parameters: _*).asInstanceOf[A])
    }).toSeq
  }

  /**
   * To find the first annotation of type A which applied on P at class level
   *
   * @param a - the class-tag of A
   * @tparam A - the type of annotation
   * @tparam P - the owner type on which annotation is applied
   * @return
   */
  def findClassAnnotation[A <: scala.annotation.Annotation: TypeTag, P: TypeTag](implicit a: ClassTag[A]): Option[A] = for {
    annotation <- typeOf[P].typeSymbol.asClass.annotations.find(_.tree.tpe =:= typeOf[A])
  } yield {
    val parameters = annotation.tree.children.tail.collect { case Literal(Constant(v: Any)) => v }
    val constructor = getAnnotationConstructor(annotation)
    constructor(parameters: _*).asInstanceOf[A]
  }

  /**
   * To find the first annotation of type A which applied on P at constructor level
   *
   * @param a - the class-tag of A
   * @tparam A - the type of annotation
   * @tparam P - the owner type on which annotation is applied
   * @return
   */
  def findConstructorAnnotation[A <: scala.annotation.Annotation: TypeTag, P: TypeTag](implicit a: ClassTag[A]): Seq[(TermSymbol, A)] = {
    val members = symbolOf[P].asClass.primaryConstructor.typeSignature.paramLists
      .flatMap(_.map(v => (v, v.annotations.find(_.tree.tpe <:< typeOf[A]))))
      .filter(_._2.isDefined).map(x => (x._1, x._2.get))

    var constructor: Option[MethodMirror] = None
    members.map(x => {
      if (constructor.isEmpty) {
        constructor = Some(getAnnotationConstructor(x._2))
      }
      val parameters = x._2.tree.children.tail.collect { case Literal(Constant(v: Any)) => v }
      (x._1.asTerm, constructor.get(parameters: _*).asInstanceOf[A])
    })
  }

  /**
   * To find the first annotation of type A which applied on P at constructor level
   *
   * @param a - the class-tag of A
   * @tparam A - the type of annotation
   * @param any - the owner object on which annotation is applied
   * @return
   */
  def findConstructorAnnotation[A <: scala.annotation.Annotation: TypeTag](any: AnyRef)(implicit a: ClassTag[A]): Seq[(TermSymbol, A)] = {
    val cls = any.getClass
    val members = runtimeMirror(cls.getClassLoader).staticClass(cls.getName).primaryConstructor.typeSignature.paramLists
      .flatMap(_.map(v => (v, v.annotations.find(_.tree.tpe <:< typeOf[A]))))
      .filter(_._2.isDefined).map(x => (x._1, x._2.get))

    var constructor: Option[MethodMirror] = None
    members.map(x => {
      if (constructor.isEmpty) {
        constructor = Some(getAnnotationConstructor(x._2))
      }
      val parameters = x._2.tree.children.tail.collect { case Literal(Constant(v: Any)) => v }
      (x._1.asTerm, constructor.get(parameters: _*).asInstanceOf[A])
    })
  }

  /**
   * To find the first annotation of type A which applied on P at member level
   *
   * @param a - the class-tag of A
   * @tparam A - the type of annotation
   * @tparam P - the owner type on which annotation is applied
   * @return
   */
  def findMembersAnnotation[A <: scala.annotation.Annotation: TypeTag, P: TypeTag](implicit a: ClassTag[A]): Seq[(TermSymbol, A)] = {
    val members = typeOf[P].members.collect { case s: TermSymbol => s }
      .filter(s => s.isVal || s.isVar).filter(_.annotations.exists(a => a.tree.tpe <:< typeOf[A]))

    var constructor: Option[MethodMirror] = None
    members.flatMap(m => m.annotations.find(_.tree.tpe =:= typeOf[A]).map((m, _))).map(x => {
      if (constructor.isEmpty) {
        constructor = Some(getAnnotationConstructor(x._2))
      }
      val parameters = x._2.tree.children.tail.collect { case Literal(Constant(v: Any)) => v }
      (x._1.asTerm, constructor.get(parameters: _*).asInstanceOf[A])
    }).toSeq
  }

  //get the constructor of the annotation
  private def getAnnotationConstructor[A <: scala.annotation.Annotation: TypeTag](inst: scala.reflect.runtime.universe.Annotation)(implicit a: ClassTag[A]): MethodMirror = {
    val anntCls = inst.tree.tpe.typeSymbol.asClass
    val anntType = inst.tree.tpe

    val mirror = runtimeMirror(a.runtimeClass.getClassLoader)
    val classMirror = mirror.reflectClass(anntCls)

    val constructor = anntType.decl(termNames.CONSTRUCTOR).asMethod
    classMirror.reflectConstructor(constructor)
  }
}
