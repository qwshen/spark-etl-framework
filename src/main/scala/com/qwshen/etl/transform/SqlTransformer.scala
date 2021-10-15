package com.qwshen.etl.transform

import com.qwshen.etl.common.SqlActor
import com.typesafe.config.Config
import org.apache.spark.sql.SparkSession

/**
 * The following is the xml definition.
 */
final class SqlTransformer extends SqlActor[SqlTransformer] {
  /**
   * Initialize the actor with the properties & config
   */
  override def init(properties: Seq[(String, String)], config: Config)(implicit session: SparkSession): Unit = {
    super.init(properties, config)

    validate(this._sqlStmt, "The sqlString or sqlFile must be defined in the pipeline for SqlTransformer.")
  }
}
