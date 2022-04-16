package com.qwshen.etl.source

import com.qwshen.etl.common.{ExecutionContext, IcebergActor}
import org.apache.spark.sql.{DataFrame, SparkSession}
import scala.util.{Failure, Success, Try}

/**
 * Load one icebarg dataset either through a table or directory.
 */
class IcebergReader extends IcebergActor[IcebergReader] {
  /**
   * Run the sql-table-reader
   *
   * @param ctx     - the execution context
   * @param session - the spark-session
   * @return
   */
  override def run(ctx: ExecutionContext)(implicit session: SparkSession): Option[DataFrame] = for {
    table <- this._table
  } yield Try {
    this._options.foldLeft(session.read.format("iceberg"))((r, o) => r.option(o._1, o._2)).load(table)
  } match {
    case Success(df) => df
    case Failure(ex) => throw new RuntimeException("Cannot load data from table - ${table}.", ex)
  }
}
