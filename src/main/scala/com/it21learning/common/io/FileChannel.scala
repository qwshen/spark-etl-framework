package com.it21learning.common.io

import scala.util._
import scala.io.Source

/**
 * FileChannel implements file related reading / writing.
 */
object FileChannel {
  def loadAsString(fileUri: String): String = {
    val source = if (fileUri.startsWith("file:/")) Source.fromURL(fileUri) else Source.fromFile(fileUri)
    try {
      source.getLines().mkString(Properties.lineSeparator)
    } finally {
      //close the handle
      source.close()
    }
  }
}
