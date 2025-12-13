package com.qwshen.etl.source

import com.qwshen.common.PropertyKey
import com.qwshen.etl.common.{IcebergActor, JobContext}
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.util.{Failure, Success, Try}

/**
 * Load one icebarg dataset either through a table or directory.
 */
class IcebergReader extends IcebergActor[IcebergReader] {
  @PropertyKey("filter", false)
  protected var _filter: Option[String] = None

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
    this._options.foldLeft(session.read.format("iceberg"))((r, o) => r.option(o._1, o._2)).load(table)
  } match {
    case Success(df) => this._filter.map(condition => df.filter(condition)).getOrElse(df)
    case Failure(ex) => throw new RuntimeException("Cannot load data from table - ${table}.", ex)
  }

  /**
   * The write mode. The valid values are: append, overwrite.
   *
   * @param value
   * @return
   */
  def filter(value: String): IcebergReader = { this._filter = Some(value); this }
}
