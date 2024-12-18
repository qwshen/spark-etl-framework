package com.qwshen.etl.common

import com.qwshen.common.PropertyInitializer
import com.qwshen.etl.functions.Decoder._
import com.qwshen.etl.functions.BinarySplitter._
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

object UdfRegister {
  /**
   * Register system UDFs
   * @param session
   */
  def register(implicit session: SparkSession): Unit = {
    import session.implicits._
    val registeredUDFs = session.catalog.listFunctions().map(func => func.name).collect()
    if (!registeredUDFs.contains("bytes_to_hex"))
      session.udf.register("bytes_to_hex", (bytes: Array[Byte], charset: String) => bytes_2_hex(bytes, charset))
    if (!registeredUDFs.contains("bytes_to_string"))
      session.udf.register("bytes_to_string", (bytes: Array[Byte], charset: String) => bytes_2_string(bytes, charset))
    if (!registeredUDFs.contains("com3_to_int"))
      session.udf.register("com3_to_int", (bytes: Array[Byte]) => com3_2_int(bytes))
    if (!registeredUDFs.contains("com3_to_double"))
      session.udf.register("com3_to_double", (bytes: Array[Byte], scale: Int) => com3_2_double(bytes, scale))
    if (!registeredUDFs.contains("binary_split"))
      session.udf.register("binary_split", (input: Array[Byte], delimiter: Array[Byte]) => bytes_split(input, delimiter))
  }
}