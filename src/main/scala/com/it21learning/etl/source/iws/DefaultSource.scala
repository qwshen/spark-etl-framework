//package com.it21learning.etl.source.iws
//
//import org.apache.spark.sql.SQLContext
//import org.apache.spark.sql.sources.{BaseRelation, RelationProvider, SchemaRelationProvider}
//import org.apache.spark.sql.types.StructType
//
//class DefaultSource extends RelationProvider with SchemaRelationProvider with Serializable {
//  /**
//   * Create the default schema
//   *
//   * @param sqlContext
//   * @param parameters
//   * @return
//   */
//  override def createRelation(sqlContext: SQLContext, parameters: Map[String, String]): BaseRelation = createRelation(sqlContext, parameters, null)
//
//  /**
//   * Create
//   * @param sqlContext
//   * @param parameters
//   * @param schema
//   * @return
//   */
//  override def createRelation(sqlContext: SQLContext, parameters: Map[String, String], schema: StructType): BaseRelation = ???
//}
