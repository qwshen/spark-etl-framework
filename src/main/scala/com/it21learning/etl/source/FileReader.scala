package com.it21learning.etl.source

import com.it21learning.etl.common.{ExecutionContext, FileReadActor}
import org.apache.spark.sql.{DataFrame, SparkSession}
import scala.util.{Failure, Success, Try}

/**
 * This FileReader is for loading json, avro & parquet files
 */
final class FileReader extends FileReadActor[FileReader] {
  /**
   * Run the file-reader
   *
   * @param ctx - the execution context
   * @param session - the spark-session
   * @return
   */
  def run(ctx: ExecutionContext)(implicit session: SparkSession): Option[DataFrame] = for {
    fmt <- this._format
    uri <- this._fileUri
  } yield Try {
    this._schema.foldLeft(this._options.foldLeft(session.read.format(fmt))((s, o) => s.option(o._1, o._2)))((r, s) => r.schema(s)).load(uri)
  } match {
    case Success(df) => df
    case Failure(ex) => throw new RuntimeException(s"Cannot load the file into data-frame - ${this._fileUri}.", ex)
  }
}
