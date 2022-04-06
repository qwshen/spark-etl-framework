package com.qwshen.etl.source

import com.qwshen.etl.common.{ExecutionContext, JdbcActor}
import org.apache.spark.sql.{DataFrame, SparkSession}
import scala.util.{Failure, Success, Try}

/**
 * Read from RDBMS
 */
class JdbcReader extends JdbcActor[JdbcReader] {
  /**
   * Run the jdbc-reader
   *
   * @param ctx - the execution context
   * @param session - the spark-session
   * @return
   */
  def run(ctx: ExecutionContext)(implicit session: SparkSession): Option[DataFrame] = Try {
    this._options.foldLeft(this._connection.foldLeft(session.read.format("jdbc"))((r, c) => r.option(c._1, c._2)))((r, o) => r.option(o._1, o._2)).load
  } match {
    case Success(df) => Some(df)
    case Failure(ex) => throw new RuntimeException(s"Cannot load from source - ${this._connection.get("dbtable")}.", ex)
  }
}
