package com.qwshen.etl.utils

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.functions.{col, count, first, lit, monotonically_increasing_id, row_number, spark_partition_id, sum}
import org.apache.spark.sql.expressions.Window

/**
 * Implement the splitting of a data-frame by # of partitions
 */
object DataframeHelper {
  private final val CN_PARTITION_ID: String = "__partition_id__"
  private final val CN_INCREASE_ID: String = "__increasing_id__"

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

      val dfResult = df.withColumn(DataframeHelper.CN_PARTITION_ID, (spark_partition_id / numPartitions).cast(IntegerType))
      (0 until finalPartitions).map(idx => dfResult.filter(col(DataframeHelper.CN_PARTITION_ID) === lit(idx)).drop(DataframeHelper.CN_PARTITION_ID))
    }
  }

  implicit class IndexDecorator(df: DataFrame) {
    def zipWithIndex(clnIndexName: String, partitionBy: Seq[String] = Nil): DataFrame = {
      var dfId = df.cache().withColumn(DataframeHelper.CN_INCREASE_ID, monotonically_increasing_id())
      partitionBy match {
        case Seq(_, _ @ _*) =>
          val clnPartitionBy = partitionBy.map(cln => col(cln))
          dfId = dfId.withColumn(clnIndexName, row_number().over(Window.partitionBy(clnPartitionBy:_*).orderBy(col(DataframeHelper.CN_INCREASE_ID))))
          dfId.drop(DataframeHelper.CN_INCREASE_ID)
        case _ =>
          dfId = dfId.withColumn(DataframeHelper.CN_PARTITION_ID, spark_partition_id())
          val dfCnt = dfId.groupBy(DataframeHelper.CN_PARTITION_ID)
              .agg(
                count(lit(1)) as "__par_cnt__",
                first(DataframeHelper.CN_INCREASE_ID) as "__par_start_no__"
              )
              .orderBy(DataframeHelper.CN_PARTITION_ID)
            .select(
              col(DataframeHelper.CN_PARTITION_ID),
              sum("__par_cnt__").over(Window.orderBy(DataframeHelper.CN_PARTITION_ID)) - col("__par_cnt__") - col("__par_start_no__") as "__par_offset__"
            )
          val dfOffset = dfId.alias("d").join(
            dfCnt.alias("c"),
            col(s"d.${DataframeHelper.CN_PARTITION_ID}") === col(s"c.${DataframeHelper.CN_PARTITION_ID}"),
            "inner"
          ).select(col("d.*"), col("c.__par_offset__"))
          dfOffset.withColumn(clnIndexName, col("__par_offset__") + col(DataframeHelper.CN_INCREASE_ID))
            .drop("__par_offset__", DataframeHelper.CN_PARTITION_ID, DataframeHelper.CN_INCREASE_ID)
      }
    }
  }
}
