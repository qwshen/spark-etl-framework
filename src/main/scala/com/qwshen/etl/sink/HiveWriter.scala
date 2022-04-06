package com.qwshen.etl.sink

import com.qwshen.common.PropertyKey
import com.qwshen.etl.common.{ExecutionContext, SqlActor}
import com.typesafe.config.Config
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{DataFrame, SparkSession}

class HiveWriter extends SqlActor[HiveWriter] {
  @PropertyKey("table", false)
  protected var _hiveTable: Option[String] = None

  @PropertyKey("partitionBy", false)
  protected var _partitionBy: Option[String] = None
  @PropertyKey("numPartitions", false)
  protected var _numPartitions: Option[Int] = None

  @PropertyKey("mode", false)
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
  override def run(ctx: ExecutionContext)(implicit session: SparkSession): Option[DataFrame] = if (this._sqlStmt.isDefined) {
    super.run(ctx)
  } else {
    for {
      ht <- this._hiveTable
      md <- this._mode
      df <- this._view.flatMap(name => ctx.getView(name))
    } yield {
      if (!session.catalog.tableExists(ht)) {
        this.error(new RuntimeException("Table [$ht] doesn't exists."))
      }

      val writer = this._partitionBy match {
        case Some(s) => this._numPartitions.map(num => df.repartition(num, s.split(",").map(c => col(c)): _*)).getOrElse(df).write
        case _ => this._numPartitions.map(num => df.coalesce(num)).getOrElse(df).write
      }
      md match {
        case "overwrite" => writer.saveAsTable(ht)
        case _ => writer.insertInto(ht)
      }
      df
    }
  }

  /**
   * Initialize the actor with the properties & config
   */
  override def init(properties: Seq[(String, String)], config: Config)(implicit session: SparkSession): Unit = {
    super.init(properties, config)

    (this._sqlStmt, this._hiveTable) match {
      case (None, None) => throw new RuntimeException("Either Sql-Statement or Hive-Table must be defined for HiveReader.")
      case (None, Some(_)) => if (this._mode.isEmpty) {
        throw new RuntimeException("When writing a hive table, the mode must be defined in HiveWriter.")
      }
      case _ =>
    }
  }
}
