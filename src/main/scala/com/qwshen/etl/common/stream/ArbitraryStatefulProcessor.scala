package com.qwshen.etl.common.stream

import com.qwshen.common.{PropertyInitializer, PropertyValidator}
import org.apache.spark.sql.DataFrame

/**
 * Define the trait for a custom stream transformer to manipulate state such as with mapGroupsWithState and/or flatMapGroupsWithState.
 * The custom stream-state transformer then is coordinated with StreamTransformer. For details, please check StreamTransformer
 */
abstract class ArbitraryStatefulProcessor extends PropertyInitializer with PropertyValidator with Serializable {
  /**
   * The method to implement the custom logic of state transformation.
   *
    * @param df
   * @return
   */
  def transformState(df: DataFrame): DataFrame
}
