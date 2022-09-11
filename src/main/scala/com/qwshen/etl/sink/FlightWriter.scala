package com.qwshen.etl.sink

import com.qwshen.common.PropertyKey
import com.qwshen.etl.common.{FlightActor, JobContext}
import org.apache.spark.sql.{DataFrame, SparkSession}
import scala.util.{Failure, Success, Try}

class FlightWriter extends FlightActor[FlightWriter] {
  //the write mode
  @PropertyKey("mode", true)
  protected var _mode: Option[String] = None

  //the source view
  @PropertyKey("view", true)
  protected var _sourceView: Option[String] = None

  /**
   * Run the flight-writer
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
    mode <- this._mode
    df <- this._sourceView.flatMap(name => ctx.getView(name))
  } yield Try {
    this._options.foldLeft(
      df.write.format("flight")
        .option("host", host).option("port", port).option("user", user).option("password", password).option("table", table)
    )((w, o) => w.option(o._1, o._2)).mode(mode).save
  } match {
    case Success(_) => df
    case Failure(ex) => throw new RuntimeException(s"Cannot write to target - ${this._host}.", ex)
  }
}
