package com.it21learning.common

trait PropertyValidater {
  /**
   * Validate one variable - not empty
   * @param variable
   * @param values
   * @param errorMsg
   * @tparam T
   * @return
   */
  def validate[T](variable: Option[T], errorMsg: String, values: Seq[T] = Nil): Option[Boolean] = variable match {
    case Some(v) if (values.isEmpty || values.contains(v)) => Some(true)
    case _ => throw new RuntimeException(errorMsg)
  }
}
