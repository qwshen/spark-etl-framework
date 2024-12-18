package com.qwshen.etl.pipeline.definition

import com.qwshen.etl.common.UdfRegister
import org.apache.spark.sql.SparkSession

/**
 * Describe one UDF registration
 * @param prefix
 * @param register
 */
case class UdfRegistration(prefix: String, register: UdfRegister)

//register all UDFs
private[etl] object UdfRegistration {
  def setup(udfRegisters: Seq[UdfRegistration])(implicit session: SparkSession): Unit = {
    //register system UDFs
    UdfRegister.register

    //register custom UDFs
    udfRegisters.foreach(r => r.register.register(r.prefix))
  }
}