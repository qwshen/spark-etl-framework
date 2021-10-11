package com.qwshen.etl.test.stream

case class OutputAge(gender: String, interested: Int,
  minAge: Option[Int], maxAge: Option[Int], averageAge: Option[Float],
  start: Option[java.sql.Timestamp], end: Option[java.sql.Timestamp])
