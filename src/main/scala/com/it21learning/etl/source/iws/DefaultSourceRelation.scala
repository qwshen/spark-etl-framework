//package com.it21learning.etl.source.iws
//
//import org.apache.spark.sql.SQLContext
//import org.apache.spark.sql.sources.BaseRelation
//import org.apache.spark.sql.types._
//
//class DefaultSourceRelation(override val sqlContext: SQLContext, userSchema: StructType) extends BaseRelation with Serializable {
//  /**
//   * Create the schema if not provided
//   * @return
//   */
//  override def schema: StructType = if (userSchema != null) userSchema else new StructType()
//    .add("msg_id", StringType, nullable = false)
//    .add("msg_direction", StringType, nullable = false).add("msg_type", StringType, nullable = true)
//    .add("to_number", StringType, nullable = false).add("sub_number", StringType, nullable = false).add("code_status", StringType, nullable = true)
//    .add("swift_1", StringType, nullable = true).add("swift_2", StringType, nullable = true).add("swift_3", StringType, nullable = true).add("swift_4", StringType, nullable = true).add("swift_5", StringType, nullable = true)
//    .add("msg_body", StringType, nullable = true)
//}
