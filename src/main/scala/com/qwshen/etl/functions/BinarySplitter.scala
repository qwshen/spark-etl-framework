package com.qwshen.etl.functions

import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.udf
import scala.collection.mutable.ArrayBuffer

object BinarySplitter {
  /**
   * Split binary array by delimiter
   * @param input
   * @param delimiter
   * @return
   */
  def bytes_split(input: Array[Byte], delimiter: Array[Byte]): Option[Array[Array[Byte]]] = if (input == null) None else {
    val output: ArrayBuffer[Array[Byte]] = ArrayBuffer[Array[Byte]]()
    if (input != null && input.length > 0 && delimiter.length > 0) {
      var (start: Int, pos: Int) = (0, 0)
      while (pos < input.length) {
        val element = input.slice(pos, Math.min(pos + delimiter.length, input.length))
        if (element sameElements delimiter) {
          output.append(input.slice(start, pos))
          start = pos + delimiter.length
          pos = start
        } else {
          pos += 1
        }
      }
      output.append(input.slice(start, input.length))
    } else {
      output.append(input)
    }
    Some(output.toArray)
  }
  /**
   * Split binary array by delimiter
   */
  val binary_split: UserDefinedFunction = udf { (input: Array[Byte], delimiter: Array[Byte]) => bytes_split(input, delimiter) }
}
