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
    if (!this._stmts.map(s => s.text.trim.replaceAll("[\r|\n]", " ").toLowerCase).map(s => this.isQuery(s)).reduce((x, y) => x | y)) {
      throw new RuntimeException("The sqlString or sqlFile in SqlReader is not a sql select or with-select-statement.")
    }
  }
}
