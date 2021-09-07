package com.it21learning.etl.sink

import com.it21learning.etl.common.DeltaWriteActor
import com.typesafe.config.Config
import org.apache.spark.sql.{DataFrame, SparkSession}
import scala.util.{Try, Success, Failure}
import scala.xml.NodeSeq

/**
 * This writer writes a data-frame to delta-lake
 *
 * The following is the definition in xml format
 *   <actor type="com.it21learning.etl.sink.DeltaWriter">
 *     <!--
 *       options for the writing
 *     -->
 *     <property name="options">
 *       <option name="replaceWhere" value="date >= '2020-05-21' and date < '2020-06-30'" />
 *       <option name="overwriteSchema" value="true" />
 *       <option name="mergeSchema" value="true" />
 *     </property>
 *     <!--
 *       The write mode - overwrite | append
 *     -->
 *     <property name="mode">overwrite|append</property>
 *     <!--
 *       The partitions to write into. Multiple columns separated by comma(,) can be specified.
 *     -->
 *     <property name="partitionBy">process_date</property>
 *     <!--
 *       The buckets to write into. Multiple columns separated by comma(,) can be specified.
 *     -->
 *     <property name="bucketing">
 *       <definition name="numBuckets">9</definition>
 *       <definition name="byColumns">user_id,event_id</definition>
 *     </property>
 *     <!--
 *       The destination where the data-frame is writing to. Only the first definition is used.
 *     -->
 *     <property name="sink">
 *       <definition name="path">/mnt/delta/events</definition>
 *       <definition name="table">events</definition>
 *     </property>
 *
 *     <!-- the target view to be written to Kafka -->
 *     <property name="view">events</property>
 *   </actor>
 */
final class DeltaWriter extends DeltaWriteActor[DeltaWriter] {
  //number of buckets
  private var _numBuckets: Option[Int] = None
  //bucket by
  private var _bucketBy: Seq[String] = Nil

  //the write mode
  private var _mode: Option[String] = None

  //write the dataframe
  protected def write(df: DataFrame): Unit = for {
    _ <- validate(this._mode, "The mode in DeltaWriter is mandatory or its value is invalid.", Seq("overwrite", "append"))
    mode <- this._mode
    _ <- validate(this._sinkType, "The sinkType in DeltaWriter is mandatory or its value is invalid.", Seq("path", "table"))
    _ <- validate(this._sinkLocation, "The sinkLocation in DeltaWriter is mandatory.")
  } {
    val initWriter = this._options.foldLeft(df.write.format("delta"))((w, o) => w.option(o._1, o._2)).mode(mode)
    //with partitionBy
    val partitionWriter = if (this._partitionBy.nonEmpty) initWriter.partitionBy(this._partitionBy: _*) else initWriter
    //with bucketBy
    val bucketWriter = (this._numBuckets, this._bucketBy) match {
      case (Some(n), Seq(c, cs @ _*)) => partitionWriter.bucketBy(n, c, cs: _*)
      case _ => partitionWriter
    }
    //write
    (this._sinkType, this._sinkLocation) match {
      case (Some(tpe), Some(path)) if (tpe == "path") => bucketWriter.save(path)
      case (Some(tpe), Some(table)) if (tpe == "table") => bucketWriter.saveAsTable(table)
      case _ =>
    }
  }

  /**
   * Initialize the file reader from the xml definition
   *
   * @param config  - the configuration object
   * @param session - the spark-session object
   */
  override def init(definition: NodeSeq, config: Config)(implicit session: SparkSession): Unit = {
    super.init(definition, config)

    (definition \ "property").foreach(prop => (prop \ "@name").text match {
      case "bucketing" => (prop \ "definition").foreach(v => (v \ "@name").text match {
        case "numBuckets" => this._numBuckets = Try(v.text.toInt) match {
          case Success(n) => Some(n)
          case Failure(t) => throw new RuntimeException("The number of buckets is invalid.")
        }
        case "byColumns" => this._bucketBy = v.text.split(",")
      })
      case "mode" => this._mode = Some(prop.text)
      case _ =>
    })

    validate(this._mode, "The mode in DeltaWriter is mandatory or its value is invalid.", Seq("overwrite", "append"))
  }

  /**
   * The columns used for bucketing when writing
   *
   * @param numBuckets
   * @param column
   * @param columns
   * @return
   */
  def bucketBy(numBuckets: Int, column: String, columns: String*): DeltaWriter = {
    this._numBuckets = Some(numBuckets)
    this._bucketBy = Seq(column) ++ columns.toSeq
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
