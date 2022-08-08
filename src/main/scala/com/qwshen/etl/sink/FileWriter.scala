package com.qwshen.etl.sink

import com.qwshen.common.PropertyKey
import com.qwshen.common.io.FileChannel
import com.qwshen.etl.common.{FileWriteActor, JobContext}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.storage.StorageLevel

import scala.util.{Failure, Success, Try}

/**
 * This writer is for writing csv, json, avro & parquet files to the target location
 */
class FileWriter extends FileWriteActor[FileWriter] {
  //the mode for the writing
  @PropertyKey("mode", true)
  protected var _mode: Option[String] = None
  @PropertyKey("emptyWrite", false)
  protected var _emptyWrite: Option[String] = None

   /**
   * Run the file-writer
   *
   * @param ctx - the execution context
   * @param session - the spark-session
   * @return
   */
  def run(ctx: JobContext)(implicit session: SparkSession): Option[DataFrame] = for {
    fmt <- this._format
    mode <- this._mode
    uri <- this._fileUri
    df <- this._view.flatMap(name => ctx.getView(name))
  } yield Try {
    val hasData: () => Boolean = () => {
      if (!(df.storageLevel.useMemory || df.storageLevel.useDisk || df.storageLevel.useOffHeap)) {
        df.persist(StorageLevel.MEMORY_AND_DISK)
      }
      df.count > 0
    }
    val goWrite: Boolean = this._emptyWrite match {
      case Some(ew) if ew.equalsIgnoreCase("no") || ew.equalsIgnoreCase("disabled") => hasData()
      case Some(ew) if ew.equalsIgnoreCase("smart") || ew.equalsIgnoreCase("default") => hasData() || !FileChannel.exists(uri)
      case _ => true
    }
    if (goWrite) {
      val initWriter = this._options.foldLeft(df.write.format(fmt))((s, o) => s.option(o._1, o._2))
      //with partitionBy
      val partitionWriter = this._partitionBy.foldLeft(initWriter)((w, cs) => w.partitionBy(cs.split(",").map(_.trim): _*))
      //write
      partitionWriter.mode(mode).save(uri)
    }
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
