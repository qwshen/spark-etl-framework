package com.qwshen.etl.sink

import com.qwshen.common.PropertyKey
import com.qwshen.etl.common.{ExecutionContext, FileWriteActor}
import org.apache.spark.sql.{DataFrame, SparkSession}
import scala.util.{Failure, Success, Try}

/**
 * This writer is for writing csv, json, avro & parquet files to the target location
 */
class FileWriter extends FileWriteActor[FileWriter] {
  //the mode for the writing
  @PropertyKey("mode", false)
  protected var _mode: Option[String] = None

  /**
   * Run the file-writer
   *
   * @param ctx - the execution context
   * @param session - the spark-session
   * @return
   */
  def run(ctx: ExecutionContext)(implicit session: SparkSession): Option[DataFrame] = for {
    fmt <- this._format
    mode <- this._mode
    uri <- this._fileUri
    df <- this._view.flatMap(name => ctx.getView(name))
  } yield Try {
    val initWriter = this._options.foldLeft(df.write.format(fmt))((s, o) => s.option(o._1, o._2))
    //with partitionBy
    val partitionWriter = this._partitionBy.foldLeft(initWriter)((w, cs) => w.partitionBy(cs.split(","): _*))
    //write
    partitionWriter.mode(mode).save(uri)
  } match {
    case Success(_) => df
    case Failure(ex) => throw new RuntimeException(s"Cannot write data to the target - $uri.", ex)
  }

  /**
   * The write mode. The valid values are: append, overwrite.
   *
   * @param value
   * @return
   */
  def mode(value: String): FileWriter = { this._mode = Some(value); this }
}
