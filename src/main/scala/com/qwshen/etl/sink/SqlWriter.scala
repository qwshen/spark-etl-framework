package com.qwshen.etl.sink

import com.qwshen.etl.common.SqlBase
import com.typesafe.config.Config
import org.apache.spark.sql.SparkSession

/**
 * The SqlWriter is to modify data with one insert, update, merge or delete statement.
 */
class SqlWriter extends SqlBase[SqlWriter] {
  /**
   * Initialize the actor with the properties & config
   */
  override def init(properties: Seq[(String, String)], config: Config)(implicit session: SparkSession): Unit = {
    super.init(properties, config)

    //verify the sql-statement is a select.
    if (this._sqlStmt.flatMap(s => "^[insert|update|merge|delete].*".r.findFirstIn(s.toLowerCase())).isEmpty) {
      throw new RuntimeException("The sqlString or sqlFile in SqlWriter is not a sql insert/update/merge/delete-statement.")
    }
  }
}
