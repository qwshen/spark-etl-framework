package com.qwshen.etl.source

import com.qwshen.etl.common.{FlightActor, JobContext}
import org.apache.spark.sql.{DataFrame, SparkSession}
import scala.util.{Failure, Success, Try}

/**
 * Read from Flight end-points
 */
class FlightReader extends FlightActor[FlightReader] {
  /**
   * Run the jdbc-reader
   *
   * @param ctx - the execution context
   * @param session - the spark-session
   * @return
   */
  def run(ctx: JobContext)(implicit session: SparkSession): Option[DataFrame] = for {
    host <- this._host
    port <- this._port
    user <- this._user
    password <- this._password
    table <- this._table
  } yield Try {
    this._options.foldLeft(session.read.format("flight").option("host", host).option("port", port).option("user", user).option("password", password).option("table", table))((r, o) => r.option(o._1, o._2)).load
  } match {
    case Success(df) => df
    case Failure(ex) => throw new RuntimeException(s"Cannot load from source - ${this._host}.", ex)
  }
}
