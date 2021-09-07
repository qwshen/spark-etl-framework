package com.it21learning.etl.test.stream

case class AgeState(gender: Option[String], interested: Option[Int],
  minAge: Option[Int], maxAge: Option[Int], sum: Option[Long], count: Option[Int],
  start: Option[java.sql.Timestamp], end: Option[java.sql.Timestamp])
