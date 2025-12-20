package com.qwshen.common.io

import scala.util._
import scala.io.Source

trait FileChannel {
  def probe(fileUri: String): Boolean = true
  def loadAsString(fileUri: String): String
}

/**
 * FileChannel implements file related reading / writing.
 */
object FileChannel extends FileChannel {
  //the registered file channels
  private var _channels: Seq[FileChannel] = Seq.empty[FileChannel]

  /**
   * Load the file as string
   * @param fileUri
   * @return
   */
  def loadAsString(fileUri: String): String = this._channels.find(c => c.probe(fileUri)) match {
      case Some(channel) => channel.loadAsString(fileUri)
      case _ =>
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
   * Register a new file channel
   *
   * @param channel
   */
  def registerChannel(channel: FileChannel): Unit = {
    this._channels = this._channels :+ channel
  }
}
