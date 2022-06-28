package com.qwshen.etl.source

import com.qwshen.etl.common.SqlBase
import com.typesafe.config.Config
import org.apache.spark.sql.SparkSession

/**
 * SqlReader is to load data from a select-statement
 */
class SqlReader extends SqlBase[SqlReader] {
  /**
   * Initialize the actor with the properties & config
   */
  override def init(properties: Seq[(String, String)], config: Config)(implicit session: SparkSession): Unit = {
    super.init(properties, config)

    //verify the sql-statement is a select.
    if (this._sqlStmt.map(s => s.replaceAll("[\r|\n]", " ").toLowerCase).forall(s => Seq("^select.+", "^with.+select.+from.+").map(e => e.r.findFirstIn(s).isEmpty).reduce((x, y) => x & y))) {
      throw new RuntimeException("The sqlString or sqlFile in SqlReader is not a sql select-statement.")
    }
  }
}
