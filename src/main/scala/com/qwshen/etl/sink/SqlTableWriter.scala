package com.qwshen.etl.sink

import com.qwshen.common.PropertyKey
import com.qwshen.etl.common.{JobContext, Actor}
import com.typesafe.config.Config
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * This is to write data to the target data-table.
 */
class SqlTableWriter extends Actor {
  @PropertyKey("table", true)
  protected var _table: Option[String] = None

  @PropertyKey("partitionBy", false)
  protected var _partitionBy: Option[String] = None
  @PropertyKey("numPartitions", false)
  protected var _numPartitions: Option[Int] = None

  @PropertyKey("mode", true)
  protected var _mode: Option[String] = None
  @PropertyKey("view", true)
  protected var _view: Option[String] = None

  /**
   * Run the file-reader
   *
   * @param ctx - the execution context
   * @param session - the spark-session
   * @return
   */
  override def run(ctx: JobContext)(implicit session: SparkSession): Option[DataFrame] = for {
    tbl <- this._table
    md <- this._mode
    df <- this._view.flatMap(name => ctx.getView(name))
  } yield {
    val writer = this._partitionBy match {
      case Some(s) => this._numPartitions.map(num => df.repartition(num, s.split(",").map(c => col(c.trim)): _*)).getOrElse(df).write
      case _ => this._numPartitions.map(num => df.coalesce(num)).getOrElse(df).write
    }
    md match {
      case "overwrite" => writer.saveAsTable(tbl)
      case _ => writer.insertInto(tbl)
    }
    df
  }

  /**
   * Initialize the actor with the properties & config
   */
  override def init(properties: Seq[(String, String)], config: Config)(implicit session: SparkSession): Unit = {
    super.init(properties, config)

    this.validate(this._mode, "The mode in SqlTableWriter is mandatory, and its value must be either overwrite or append.", Seq("overwrite", "append"))
  }
}
