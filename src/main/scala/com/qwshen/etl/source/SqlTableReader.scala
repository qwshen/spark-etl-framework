package com.qwshen.etl.source

import com.qwshen.common.PropertyKey
import com.qwshen.etl.common.{Actor, JobContext}
import org.apache.spark.sql.{DataFrame, SparkSession}
import scala.util.{Failure, Success, Try}

/**
 * Load a source table with one optional filter.
 */
class SqlTableReader extends Actor {
  @PropertyKey("table", true)
  protected var _table: Option[String] = None
  @PropertyKey("filter", false)
  protected var _filterPredicate: Option[String] = None

  /**
   * Run the sql-table-reader
   *
   * @param ctx     - the execution context
   * @param session - the spark-session
   * @return
   */
  override def run(ctx: JobContext)(implicit session: SparkSession): Option[DataFrame] = for {
    table <- this._table
  } yield Try {
    this._filterPredicate.foldLeft(session.table(table))((r, p) => r.filter(p))
  } match {
    case Success(df) => df
    case Failure(ex) => throw new RuntimeException("Cannot load data from table - ${table}.", ex)
  }
}