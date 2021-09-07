package com.it21learning.etl.source

import com.it21learning.etl.common.{Actor, ExecutionContext}
import org.apache.spark.sql.{DataFrame, SparkSession}

final class HiveReader extends Actor {
  private var _hiveTable: Option[String] = None
  private var _filterPredicate: Option[String] = None

  /**
   * Run the file-reader
   *
   * @param ctx - the execution context
   * @param session - the spark-session
   * @return
   */
  def run(ctx: ExecutionContext)(implicit session: SparkSession): Option[DataFrame] = for {
    ht <- this._hiveTable
  } yield {
    if (!session.catalog.tableExists(ht)) {
      this.error(new RuntimeException("Table [$ht] doesn't exists."))
    }
    this._filterPredicate.foldLeft(session.table(ht))((r, p) => r.filter(p))
  }
}
