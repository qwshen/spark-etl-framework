package com.qwshen.etl.source

import com.qwshen.etl.common.{JobContext, FileReadActor}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.{input_file_name, count, lit}
import scala.util.{Failure, Success, Try}

/**
 * This FileReader is for loading json, avro & parquet files
 */
class FileReader extends FileReadActor[FileReader] {
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
    this._schema.foldLeft(this._options.foldLeft(session.read.format(fmt))((s, o) => s.option(o._1, o._2)))((r, s) => r.schema(s)).load(uri)
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
    import df.sparkSession.implicits._
    df.select(input_file_name().as("___input_fn__"))
      .groupBy($"___input_fn__")
      .agg(count(lit(1)).as("___input_fn_cnt__"))
    .select($"___input_fn__", $"___input_fn_cnt__").collect().zipWithIndex
      .map { case(r, i) => ((r(0).toString, String.format("%s", r(1).toString)), i) }
      .flatMap { case(r, i) => Seq((s"input-file${i + 1}-name", r._1), (s"input-file${i + 1}-row-count", r._2)) }
  }
}
