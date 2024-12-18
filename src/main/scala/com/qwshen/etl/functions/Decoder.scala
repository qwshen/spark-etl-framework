package com.qwshen.etl.functions

import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions._
import scala.math.pow

object Decoder {
  /**
   * convert bytes to hex string
   * @param input
   * @param charset
   * @return
   */
  def bytes_2_hex(input: Seq[Byte], charset: String = "ascii"): Option[String] = if (input == null) None else {
    val HEX_ARRAY = "0123456789abcdef".getBytes(charset);

    val output = new Array[Byte](input.length * 2)
    for (i <- input.indices) {
      val v = input(i) & 0xFF
      output(i * 2) = HEX_ARRAY(v >>> 4)
      output(i * 2 + 1) = HEX_ARRAY(v & 0x0F)
    }
    Some(new String(output, charset))
  }
  /**
   * Decode input-bytes to hex string
   */
  val bytes_to_hex: UserDefinedFunction = udf(bytes_2_hex _)

  /**
   * Decode input-bytes with designated charset
   */
  def bytes_2_string(bytes: Array[Byte], charset: String = "ascii"): Option[String] = if (bytes == null) None else Some(new String(bytes, charset))
  /**
   * UDF: bytes_to_string
   */
  val bytes_to_string: UserDefinedFunction = udf { bytes_2_string _ }

  /**
   * Decode input-bytes (packed decimal) as double
   */
  def com3_2_double(bytes: Array[Byte], scale: Int): Option[Double] = if (bytes == null) None else bytes_2_hex(bytes).map(hexString => {
    val sign = if (hexString.last == 'c') 1 else -1
    hexString.init.toDouble / pow(10, scale) * sign
  })
  /**
   * UDF: com3_to_double
   */
  val com3_to_double: UserDefinedFunction = udf { com3_2_double _ }

  /**
   * Decode input-bytes (packed decimal) as int
   */
  def com3_2_int(bytes: Array[Byte]): Option[Int] = if (bytes == null) None else bytes_2_hex(bytes).map(hexString => {
    val sign = if (hexString.last == 'c') 1 else -1
    hexString.init.toInt * sign
  })
  /**
   * UDF: com3_to_int
   */
  val com3_to_int: UserDefinedFunction = udf { com3_2_int _ }
}
