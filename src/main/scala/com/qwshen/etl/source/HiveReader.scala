package com.qwshen.etl.source

import com.qwshen.common.PropertyKey
import com.qwshen.etl.common.{ExecutionContext, SqlActor}
import com.typesafe.config.Config
import org.apache.spark.sql.{DataFrame, SparkSession}

final class HiveReader extends SqlActor[HiveReader] {
  @PropertyKey("table", false)
  private var _hiveTable: Option[String] = None
  @PropertyKey("filter", false)
  private var _filterPredicate: Option[String] = None

  /**
   * Run the file-reader
   *
   * @param ctx - the execution context
   * @param session - the spark-session
   * @return
   */
  override def run(ctx: ExecutionContext)(implicit session: SparkSession): Option[DataFrame] = this._hiveTable match {
    case Some(ht) =>
      if (!session.catalog.tableExists(ht)) {
        this.error(new RuntimeException(s"Table [$ht] doesn't exists."))
      }
      Some(this._filterPredicate.foldLeft(session.table(ht))((r, p) => r.filter(p)))
    case _ => super.run(ctx)
  }

  /**
   * Initialize the actor with the properties & config
   */
  override def init(properties: Seq[(String, String)], config: Config)(implicit session: SparkSession): Unit = {
    super.init(properties, config)

    (this._sqlStmt, this._hiveTable) match {
      case (None, None) => throw new RuntimeException("Either Sql-Statement or Hive-Table must be defined for HiveReader.")
      case _ =>
    }
  }
}
