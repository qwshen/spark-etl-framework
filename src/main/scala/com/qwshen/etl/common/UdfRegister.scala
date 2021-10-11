package com.qwshen.etl.common

import com.qwshen.common.PropertyInitializer
import org.apache.spark.sql.SparkSession

trait UdfRegister extends PropertyInitializer {
  /*
    To register an udf.

    For example:
      //prefix: my_
      spark.udf.register("my_Upper", (s: String) => s.toUpperCase)
   */
  def register(prefix: String)(implicit session: SparkSession): Unit
}
