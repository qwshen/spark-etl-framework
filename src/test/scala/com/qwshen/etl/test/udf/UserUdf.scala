package com.qwshen.etl.test.udf

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions._

class UserUdf extends com.qwshen.etl.common.UdfRegister {
  def register(prefix: String)(implicit session: SparkSession): Unit = {
    session.udf.register(s"${prefix}_gender", UserUdf.f_gender)
  }
}

object UserUdf {
  val f_gender: UserDefinedFunction = udf {
    (gender: String) => gender match {
      case "M" | "m" | "Male" | "MALE" | "male" => "M"
      case "F" | "f" | "Female" | "FEMALE" | "female" => "F"
      case _ => "N/A"
    }
  }
}