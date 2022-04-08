package com.qwshen.common.io

import scala.util._
import scala.io.Source

/**
 * FileChannel implements file related reading / writing.
 */
object FileChannel {
  def loadAsString(fileUri: String): String = {
    val source = "^[a-z0-9A-Z]+:/.*$".r.findFirstMatchIn(fileUri) match {
      case Some(_) => Source.fromURL(fileUri)
      case _ => Source.fromFile(fileUri)
    }
    try {
      source.getLines().mkString(Properties.lineSeparator)
    } finally {
      //close the handle
      source.close()
    }
  }
}
