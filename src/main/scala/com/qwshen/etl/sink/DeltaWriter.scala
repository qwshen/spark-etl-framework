package com.qwshen.etl.sink

import com.qwshen.common.PropertyKey
import com.qwshen.etl.common.DeltaWriteActor
import org.apache.spark.sql.DataFrame

/**
 * This writer writes a data-frame to delta-lake
 */
class DeltaWriter extends DeltaWriteActor[DeltaWriter] {
  //number of buckets
  @PropertyKey("bucket.numBuckets", false)
  protected var _numBuckets: Option[Int] = None
  //columns separated by comma for bucket-by
  @PropertyKey("bucket.by", false)
  protected var _bucketBy: Option[String] = None

  //the write mode
  @PropertyKey("mode", true)
  protected var _mode: Option[String] = None

  //write the dataframe
  protected def write(df: DataFrame): Unit = for {
    mode <- this._mode
  } {
    val initWriter = this._options.foldLeft(df.write.format("delta"))((w, o) => w.option(o._1, o._2))
    //with partitionBy
    val partitionWriter = this._partitionBy match {
      case Some(cs) => initWriter.partitionBy(cs.split(","): _*)
      case _ => initWriter
    }
    //with bucketBy
    val bucketWriter = (this._numBuckets, this._bucketBy) match {
      case (Some(n), Some(cs)) =>
        val columns = cs.split(",").toSeq
        partitionWriter.bucketBy(n, columns.head, columns.tail: _*)
      case _ => partitionWriter
    }
    //write
    (this._sinkTable, this._sinkPath) match {
      case (Some(table), _) => mode match {
        case "overwrite" => bucketWriter.saveAsTable(table)
        case _ => bucketWriter.insertInto(table)
      }
      case (_, Some(path)) => bucketWriter.mode(mode).save(path)
      case _ => throw new RuntimeException("The sinkPath or sinkTable cannot be both empty.")
    }
  }

  /**
   * The columns used for bucketing when writing
   */
  def bucketBy(numBuckets: Int, column: String, columns: String*): DeltaWriter = {
    this._numBuckets = Some(numBuckets)
    this._bucketBy = Some((Seq(column) ++ columns.toSeq).mkString(","))
    this
  }

  /**
   * The write mode. The valid values are: append, overwrite.
   *
   * @param value
   * @return
   */
  def writeMode(value: String): DeltaWriter = { this._mode = Some(value); this }
}
