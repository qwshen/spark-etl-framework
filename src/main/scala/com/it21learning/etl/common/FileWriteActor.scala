package com.it21learning.etl.common

import com.it21learning.common.PropertyKey

/**
 * The FileWriteActor is a common base class for FileWriter & FileStreamWriter.
 */
private[etl] abstract class FileWriteActor[T] extends Actor { self: T =>
  //the file format
  @PropertyKey("format", true)
  protected var _format: Option[String] = None

  //the options for loading the file
  @PropertyKey("options.*", false)
  protected var _options: Map[String, String] = Map.empty[String, String]

  //partition-by: the columns separated by comma(,) used to partition data when writing.
  @PropertyKey("partitionBy", false)
  protected var _partitionBy: Option[String] = None

  //the file location
  @PropertyKey("fileUri", true)
  protected var _fileUri: Option[String] = None

  //the view for writing out
  @PropertyKey("view", true)
  protected var _view: Option[String] = None

  /**
   * The format of the source files.
   *
   * @param format
   * @return
   */
  def targetFormat(format: String): T = { this._format = Some(format); this }

  /**
   * The load option
   *
   * @param name
   * @param value
   * @return
   */
  def writeOption(name: String, value: String): T = { this._options = this._options + (name -> value); this }

  /**
   * The load options
   *
   * @param options
   * @return
   */
  def writeOptions(options: Map[String, String]): T = { this._options = this._options ++ options; this }

  /**
   * Partition by
   *
   * @param column
   * @param columns
   * @return
   */
  def partitionBy(column: String, columns: String*): T = { this._partitionBy = Some((Seq(column) ++ columns.toSeq).mkString(",")); this }

  /**
   * The source path of the files
   *
   * @param path
   * @return
   */
  def fileUri(path: String): T = { this._fileUri = Some(path); this }

  /**
   * The view of its data to be written out
   *
   * @param view
   * @return
   */
  def sourceView(view: String): T = { this._view = Some(view); this }
}
