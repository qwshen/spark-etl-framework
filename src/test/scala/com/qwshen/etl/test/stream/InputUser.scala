package com.qwshen.etl.test.stream

case class InputUser(userId: String, gender: Option[String], age: Option[Int],
  interested: Option[Int], processDate: Option[String],
  timeStamp: java.sql.Timestamp, windowStart: java.sql.Timestamp, windowEnd: java.sql.Timestamp)
