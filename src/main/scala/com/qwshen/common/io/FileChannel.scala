package com.qwshen.common.io

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.SparkSession
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

  /**
   * Check if the path already there
   * @param path - the path of a file or directory
   * @param session - the Spark-Session object
   * @return - true if the path exists, otherwise false
   */
  def exists(path: String)(implicit session: SparkSession): Boolean = FileSystem.get(session.sparkContext.hadoopConfiguration).exists(new Path(path))
}
