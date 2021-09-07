package com.it21learning.etl.sink

import com.it21learning.etl.common.{Actor, ExecutionContext}
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{DataFrame, SparkSession}

final class HiveWriter extends Actor {
  private var _hiveTable: Option[String] = None

  private var _partitionBy: Seq[String] = Nil
  private var _numPartitions: Option[Int] = None

  private var _mode: Option[String] = None
  private var _view: Option[String] = None

  /**
   * Run the file-reader
   *
   * @param ctx - the execution context
   * @param session - the spark-session
   * @return
   */
  def run(ctx: ExecutionContext)(implicit session: SparkSession): Option[DataFrame] = for {
    ht <- this._hiveTable
    md <- this._mode
    df <- this._view.flatMap(name => ctx.getView(name))
  } yield {
    if (!session.catalog.tableExists(ht)) {
      this.error(new RuntimeException("Table [$ht] doesn't exists."))
    }

    (this._partitionBy match {
      case Seq(_, _ @ _*) => this._numPartitions.map(num => df.repartition(num, this._partitionBy.map(c => col(c)): _*)).getOrElse(df).write
      case _ => this._numPartitions.map(num => df.coalesce(num)).getOrElse(df).write
    }).mode(md).saveAsTable(ht)

    df
  }
}
