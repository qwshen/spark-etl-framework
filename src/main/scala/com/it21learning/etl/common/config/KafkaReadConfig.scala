package com.it21learning.etl.common.config

import com.it21learning.common.PropertyKey

/**
 * The kafka read configuration
 *
 * @param bootStrapServers
 * @param topics
 */
case class KafkaReadConfig(
  @PropertyKey("bootstrapServers", false) val bootStrapServers: String,
  @PropertyKey("topics", false) val topics: String
) {
  //the options for loading the file
  @PropertyKey("options", true)
  val options: Map[String, String] = Map.empty[String, String]

  //key schema
  @PropertyKey("keySchema", true)
  val keySchema: Option[String] = None

  //value schema
  @PropertyKey("valueSchema", true)
  val valueSchema: Option[String] = None
}
