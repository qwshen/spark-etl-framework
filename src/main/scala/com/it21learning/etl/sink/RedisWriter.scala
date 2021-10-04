package com.it21learning.etl.sink

import com.it21learning.common.PropertyKey
import com.it21learning.etl.common.{RedisActor, ExecutionContext}
import org.apache.spark.sql.{DataFrame, SparkSession}
import scala.util.{Failure, Success, Try}

/**
 * Write to Redis. Note - the redis requires to be 5.0.5+.
 */
final class RedisWriter extends RedisActor[RedisWriter] {
  //mode
  @PropertyKey("mode", true)
  private var _mode: Option[String] = None
  //view
  @PropertyKey("view", true)
  private var _view: Option[String] = None

  /**
   * Run the jdbc-reader
   *
   * @param ctx - the execution context
   * @param session - the spark-session
   * @return
   */
  def run(ctx: ExecutionContext)(implicit session: SparkSession): Option[DataFrame] = for {
    host <- this._host
    port <- this._port
    dbNum <- this._dbNum
    table <- this._table
    mode <- this._mode
    df <- this._view.flatMap(name => ctx.getView(name))
  } yield Try {
    var options = Map("host" -> host, "port" -> port.toString, "dbNum" -> dbNum.toString, "table" -> table)
    this._authPassword match {
      case Some(pwd) => options = options + ("auth" -> pwd)
      case _ =>
    }
    (options ++ this._options).foldLeft(df.write.format("org.apache.spark.sql.redis"))((w, o) => w.option(o._1, o._2)).mode(mode).save
  } match {
    case Success(_) => df
    case Failure(ex) => throw new RuntimeException(s"Cannot load from source - ${this._table}.", ex)
  }

  /**
   * The write mode
   * @param mode
   * @return
   */
  def writeMode(mode: String): RedisWriter = { this._mode = Some(mode); this }

  /**
   * The source view to be written
   * @param view
   * @return
   */
  def sourceView(view: String): RedisWriter = { this._view = Some(view); this }
}
