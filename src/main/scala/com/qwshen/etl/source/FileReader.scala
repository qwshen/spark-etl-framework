package com.qwshen.etl.source

import com.qwshen.etl.common.{FileReadActor, JobContext}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.{col, count, input_file_name, lit}
import org.apache.spark.storage.StorageLevel
import scala.util.{Failure, Success, Try}

/**
 * This FileReader is for loading json, avro & parquet files
 */
class FileReader extends FileReadActor[FileReader] {
  private final val _clmnFileName: String = "___input_fn__"
  private final val _clmnFileCnt: String = "___input_fn_cnt__"

  /**
   * Run the file-reader
   *
   * @param ctx - the execution context
   * @param session - the spark-session
   * @return
   */
  def run(ctx: JobContext)(implicit session: SparkSession): Option[DataFrame] = for {
    fmt <- this._format
    uri <- this._fileUri
  } yield Try {
    val df = this._schema.foldLeft(this._options.foldLeft(session.read.format(fmt))((s, o) => s.option(o._1, o._2)))((r, s) => r.schema(s)).load(uri)
    if (ctx.metricsRequired) df.withColumn(this._clmnFileName, input_file_name()) else df
  } match {
    case Success(df) => df
    case Failure(ex) => throw new RuntimeException(s"Cannot load the file into data-frame - ${this._fileUri}.", ex)
  }

  /**
   * Calculate the rows count of each file
   * @param df
   *  @return
   */
  override def collectMetrics(df: DataFrame): Seq[(String, String)] = {
    if (!(df.storageLevel.useMemory || df.storageLevel.useDisk || df.storageLevel.useOffHeap)) {
      df.persist(StorageLevel.MEMORY_AND_DISK)
    }

    df.groupBy(col(this._clmnFileName))
      .agg(count(lit(1)).as(this._clmnFileCnt))
    .select(col(this._clmnFileName), col(this._clmnFileCnt)).collect().zipWithIndex
      .map { case(r, i) => ((r(0).toString, String.format("%s", r(1).toString)), i) }
      .flatMap { case(r, i) => Seq((s"input-file${i + 1}-name", r._1), (s"input-file${i + 1}-row-count", r._2)) }
  }
}
