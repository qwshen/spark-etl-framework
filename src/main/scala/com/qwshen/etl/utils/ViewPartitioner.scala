package com.qwshen.etl.utils

import com.qwshen.common.PropertyKey
import com.qwshen.etl.common.{Actor, JobContext}
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{DataFrame, SparkSession}
import scala.util.{Failure, Success, Try}

/**
 * The ViewPartitioner is to partition a view (dataframe) into the # of partitions by the optional columns.
 */
class ViewPartitioner extends Actor {
  @PropertyKey("partitionBy", false)
  private var _partitionBy: Option[String] = None
  @PropertyKey("numPartitions", true)
  private var _numPartitions: Option[Int] = None

  @PropertyKey("view", true)
  private var _view: Option[String] = None

  /**
   * Run the jdbc-writer
   *
   * @param ctx - the execution context
   * @param session - the spark-session
   * @return
   */
  def run(ctx: JobContext)(implicit session: SparkSession): Option[DataFrame] = for {
    df <- this._view.flatMap(name => ctx.getView(name))
    np <- this._numPartitions
  } yield Try {
    this._partitionBy match {
      case Some(columns) => df.repartition(np, columns.split(",").map(c => col(c.trim)): _*)
      case _ => df.repartition(np)
    }
  } match {
    case Success(_) => df
    case Failure(ex) => throw new RuntimeException(s"Cannot repartition the view - ${this._view}.", ex)
  }

  /**
   * The columns used for partitioning
   * @param columns
   * @return
   */
  def partitionBy(columns: String): ViewPartitioner = { this._partitionBy = Some(columns); this }

  /**
   * The number of partitions after the partitioning
   * @param number
   * @return
   */
  def numPartitions(number: Int): ViewPartitioner = { this._numPartitions = Some(number); this }

  /**
   * The view to be partitioned
   * @param view
   * @return
   */
  def forView(view: String): ViewPartitioner = { this._view = Some(view); this }
}
