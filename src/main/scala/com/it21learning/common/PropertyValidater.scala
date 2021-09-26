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

  /**
   * Validate if all variables are empty
   * @param variables
   * @param errorMsg
   * @tparam T
   * @return
   */
  def validate[T](variables: Seq[Option[T]], errorMsg: String): Option[Boolean] = if (variables.foldLeft(false)((r, m) => r || m.nonEmpty)) {
    Some(true)
  } else {
    throw new RuntimeException(errorMsg)
  }

  /**
   * Validate if variables contains all key in keys
   * @param variables
   * @param keys
   * @param errorMsg
   * @tparam T
   * @return
   */
  def validate[T](variables: Map[String, T], keys: Seq[String], errorMsg: String): Option[Boolean] = if (keys.forall(k => variables.contains(k))) {
    Some(true)
  } else {
    throw new RuntimeException(errorMsg)
  }
}
