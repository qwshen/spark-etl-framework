package com.qwshen.etl.utils

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.functions.{spark_partition_id, lit}

/**
 * Implement the splitting of a data-frame by # of partitions
 */
object DataframeSplitter {
  implicit class SplitDecorator(df: DataFrame) {
    /**
     * Split an input dataframe into multiple dataframes as each may contain up to the numPartitions of partitions
     * @param numPartitions - the # of partition is each unit after splitting
     * @return
     */
    def split(numPartitions: Int): Seq[DataFrame] = {
      var finalPartitions = df.rdd.partitions.length / numPartitions
      if ((df.rdd.partitions.length % numPartitions) > 0) {
        finalPartitions = finalPartitions + 1
      }

      import df.sparkSession.implicits._
      val dfResult = df.withColumn("__partition_id__", (spark_partition_id / numPartitions).cast(IntegerType))
      (0 until finalPartitions).map(idx => dfResult.filter($"__partition_id__" === lit(idx)).drop("__partition_id__"))
    }
  }
}
