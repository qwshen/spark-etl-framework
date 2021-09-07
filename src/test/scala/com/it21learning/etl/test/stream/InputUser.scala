package com.it21learning.etl.test.stream

case class InputUser(userId: String, gender: Option[String], age: Option[Int],
  invited: Option[Int], interested: Option[Int], processDate: Option[String],
  timeStamp: java.sql.Timestamp, windowStart: java.sql.Timestamp, windowEnd: java.sql.Timestamp)
