package com.it21learning.etl.source

import com.it21learning.etl.common.{DeltaReadActor, ExecutionContext}
import org.apache.spark.sql.{DataFrame, SparkSession}
import scala.util.{Failure, Success, Try}

/**
 * This reader reads data from delta lake into a data-frame.
 */
final class DeltaReader extends DeltaReadActor[DeltaReader] {
  /**
   * Execute the action
   *
   * @param ctx     - the execution context
   * @param session - the spark-session
   *  @return
   */
  def run(ctx: ExecutionContext)(implicit session: SparkSession): Option[DataFrame] = Try {
    //the initial DataframeReader
    val reader = this._options.foldLeft(session.read.format("delta"))((r, o) => r.option(o._1, o._2))
    //load
    (this._sourceTable, this._sourcePath) match {
      case (Some(table), _) => reader.table(table)
      case (_, Some(path)) => reader.load(path)
      case _ => throw new RuntimeException("The source table and source path are defined.")
    }
  } match {
    case Success(df) => Some(df)
    case Failure(t) => throw new RuntimeException(s"Load from delta ${this._sourceTable.getOrElse(this._sourcePath.getOrElse(""))} failed.", t)
  }
}
