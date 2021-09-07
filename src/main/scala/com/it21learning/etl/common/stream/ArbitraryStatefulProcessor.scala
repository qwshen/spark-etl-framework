package com.it21learning.etl.common.stream

import com.it21learning.common.PropertyComponent
import org.apache.spark.sql.DataFrame

/**
 * Define the trait for a custom stream transformer to manipulate state such as with mapGroupsWithState and/or flatMapGroupsWithState.
 * The custom stream-state transformer then is coordinated with StreamTransformer. For details, please check StreamTransformer
 */
abstract class ArbitraryStatefulProcessor extends PropertyComponent with Serializable {
  /**
   * The method to implement the custom logic of state transformation.
   *
    * @param df
   * @return
   */
  def transformState(df: DataFrame): DataFrame
}
