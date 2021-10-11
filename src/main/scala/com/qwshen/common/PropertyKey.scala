package com.qwshen.common

/**
 * For annotating members in a class.
 *
 * @param path - the key path
 * @param required - whether or not the property is required.
 */
case class PropertyKey(path: String, required: Boolean = false) extends scala.annotation.StaticAnnotation