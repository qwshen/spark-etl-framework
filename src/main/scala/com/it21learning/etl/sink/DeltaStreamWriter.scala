package com.it21learning.etl.sink

import com.it21learning.etl.common.DeltaWriteActor
import com.typesafe.config.Config
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.util.Try
import scala.xml.NodeSeq

/**
 * This writer writes a data-frame to delta-lake in streaming mode.
 *
 * The following is the definition in xml format
 *   <actor type="com.it21learning.etl.sink.DeltaStreamWriter">
 *     <!-- options for the writing -->
 *     <property name="options">
 *       <option name="checkpointLocation" value="/mnt/checkpoint/events" />
 *     </property>
 *
 *     <!-- The streaming outputMode - complete|update|append. This is mandatory -->
 *     <property name="outputMode">complete</property>
 *     <!-- How long to wait for the streaming to execute before shut it down. This is only for test-debug purpose -->
 *     <property name="waitTimeInMs">16000</property>
 *
 *     <!-- The trigger defines how often to write -->
 *     <property name="trigger">
 *       <definition name="mode">continuous|processingTime|once</definition>
 *       <definition name="interval">5 seconds</definition>
 *     </property>
 *     <!-- The partitions to write into -->
 *     <property name="partitionBy">process_date</property>

 *     <!--
 *       The destination where the data-frame is writing to. Once the first definition is used.
 *       Please note: the sink table is not supported as of delta v0.7.0.
 *     -->
 *     <property name="sink">
 *       <definition name="path">/mnt/delta/events</definition>
 *       <definition name="table">events</definition>
 *     </property>
 *
 *     <!-- the source view to be written to Kafka -->
 *     <property name="view">events</property>
 *   </actor>
 */
final class DeltaStreamWriter extends DeltaWriteActor[DeltaStreamWriter] {
  //output mode
  private var _outputMode: Option[String] = None

  //trigger mode
  private var _triggerMode: Option[String] = None
  //trigger interval
  private var _triggerInterval: Option[String] = None

  //wait time in ms for test
  private var _waittimeInMs: Option[Long] = None

  //write the dataframe
  protected def write(df: DataFrame): Unit = for {
    _ <- validate(this._outputMode, "The outputMode in DeltaStreamWriter is mandatory or its value is invalid.", Seq("complete", "append", "update"))
    mode <- this._outputMode
    _ <- validate(this._sinkType, "The sinkType in DeltaWriter is mandatory or its value is invalid.", Seq("path", "table"))
    _ <- validate(this._sinkLocation, "The sinkLocation in DeltaWriter is mandatory.")
  } {
    val initQuery = this._options.foldLeft(df.writeStream.format("delta"))((w, o) => w.option(o._1, o._2)).outputMode(mode)
    //with partitionBy
    val partitionQuery = if (this._partitionBy.nonEmpty) initQuery.partitionBy(this._partitionBy: _*) else initQuery
    //combine with trigger
    val triggerQuery = (this._triggerMode, this._triggerInterval) match {
      case (Some(m), Some(t)) if (m == "continuous") => partitionQuery.trigger(Trigger.Continuous(t))
      case (Some(m), Some(t)) if (m == "processingTime") => partitionQuery.trigger(Trigger.ProcessingTime(t))
      case (Some(m), _) if (m == "once") => partitionQuery.trigger(Trigger.Once())
      case _ => partitionQuery
    }
    //write
    (this._sinkType, this._sinkLocation) match {
      case (Some(tpe), Some(path)) if (tpe == "path") => this._waittimeInMs match {
        case Some(ts) => triggerQuery.start(path).awaitTermination(ts)
        case _ => triggerQuery.start(path).awaitTermination()
      }
      //the following sink table is not supported as of delta v0.7.0
      //case (Some(tpe), Some(table)) if (tpe == "table") => this._waittimeInMs match {
      //  case Some(ts) => triggerQuery.saveAsTable(table).awaitTermination(ts)
      //  case _ => triggerQuery.saveAsTable(table).awaitTermination()
      //}
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

    //scan bootstrap-servers & topics first
    (definition \ "property").foreach(prop => (prop \ "@name").text match {
      case "outputMode" => this._outputMode = Some(prop.text)
      case "trigger" => (prop \ "definition").foreach(vd => (vd \ "@name").text match {
        case "mode" => this._triggerMode = Some(vd.text)
        case "interval" => this._triggerInterval = Some(vd.text)
        case _ =>
      })
      case "waitTimeInMs" => this._waittimeInMs = Try(prop.text.toLong).toOption
      case _ =>
    })

    validate(this._outputMode, "The outputMode in DeltaStreamWriter is mandatory or its value is invalid.", Seq("complete", "append", "update"))
  }

  /**
   * Define the output-mode. The valid values are: complete, append, update
   *
   * @param value
   * @return
   */
  def outputMode(value: String): DeltaStreamWriter = { this._outputMode = Some(value); this }

  /**
   * The trigger mode
   *
   * @param mode
   * @return
   */
  def triggerMode(mode: String): DeltaStreamWriter = { this._triggerMode = Some(mode); this }

  /**
   * The trigger interval
   * @param duration
   * @return
   */
  def triggerInterval(duration: String): DeltaStreamWriter = { this._triggerInterval = Some(duration); this }

  /**
   * Wait time for streaming to execute before shut it down
   *
   * @param waittime
   * @return
   */
  def waitTimeInMs(waittime: Long): DeltaStreamWriter = { this._waittimeInMs = Some(waittime); this }
}
