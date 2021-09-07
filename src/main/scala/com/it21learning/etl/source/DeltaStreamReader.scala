package com.it21learning.etl.source

import com.it21learning.etl.common.{DeltaReadActor, ExecutionContext}
import com.typesafe.config.Config
import org.apache.spark.sql.functions.current_timestamp
import org.apache.spark.sql.{DataFrame, SparkSession}
import scala.util.{Failure, Success, Try}
import scala.xml.NodeSeq

/**
 * This reader reads data from delta lake into a data-frame in streaming mode.
 *
 * The following is the definition in xml format
 *   <actor type="com.it21learning.etl.source.DeltaStreamReader">
 *     <!-- options for the reading -->
 *     <property name="options">
 *       <option name="ignoreDeletes" value="true" />
 *       <option name="ignoreChanges" value="true" />
 *       <option name="startingVersion" value="11" />
 *       <option name="startingTimestamp" value="2018-09-18T11:15:21.021Z" />
 *       <option name="startingTimestamp" value="current_timestamp() - interval 24 hours" />
 *       <option name="startingTimestamp" value="date_sub(current_date(), 1)" />
 *       <option name="maxBytesPerTrigger" value="40960" />
 *       <option name="maxFilesPerTrigger" value="64" />
 *     </property>
 *     <!-- The source location where the data-frame is reading from. Only the first definition is used. This is mandatory. -->
 *     <property name="source">
 *       <definition name="path">/mnt/delta/events</definition>
 *       <!-- the table source is not supported as of delta v0.7.0 -->
 *       <definition name="table">events</definition>
 *     </property>
 *
 *     <!-- the watermark for the streaming read. -->
 *     <property> name="waterMark">
 *       <definition name="timeField">event_time</definition>
 *       <definition name="delayThreshold">10 seconds</definition>
 *     </property>
 *     <!-- to add an additional timestamp in the name of __timestamp -->
 *     <property name="addTimestamp">false</property>
 *   </actor>
 */
final class DeltaStreamReader extends DeltaReadActor[DeltaStreamReader] {
  //add timestamp
  private var _addTimestamp: Boolean = false

  //water-mark time field
  private var _wmTimeField: Option[String] = None
  //water-mark delay duration
  private var _wmDelayThreshold: Option[String] = None

  /**
   * Execute the action
   *
   * @param ctx     - the execution context
   * @param session - the spark-session
   *  @return
   */
  def run(ctx: ExecutionContext)(implicit session: SparkSession): Option[DataFrame] = Try {
    //the initial DataframeReader
    val reader = this._options.foldLeft(session.readStream.format("delta"))((r, o) => r.option(o._1, o._2))
    //load
    val df = (this._sourceType, this._sourceLocation) match {
      case (Some(tpe), Some(path)) if (tpe == "path") => reader.load(path)
      //the following table source is not supported for now (delta v0.7.0).
      //case (Some(tpe), Some(table)) if (tpe == "table") => reader.table(table)
      case _ => throw new RuntimeException("The source type and/or source location is/are invalid. Note, please do not use table source for now.")
    }

    //plug in the special __timestamp with current-timestamp
    val dfResult = if (this._addTimestamp) df.withColumn("__timestamp", current_timestamp) else df
    //enable water-mark if required
    (this._wmTimeField, this._wmDelayThreshold) match {
      case (Some(m), Some(t)) => dfResult.withWatermark(m, t)
      case _ => dfResult
    }
  } match {
    case Success(df) => Some(df)
    case Failure(t) => throw new RuntimeException(s"Load from delta ${this._sourceLocation.getOrElse("")} failed.", t)
  }

  /**
   * Initialize the file reader from the xml definition
   *
   * @param config     - the configuration object
   * @param session    - the spark-session object
   */
  override def init(definition: NodeSeq, config: Config)(implicit session: SparkSession): Unit = {
    super.init(definition, config)

    //parse for the water-mark information
    for {
      prop <- (definition \ "property")
      if ((prop \ "@name").text == "waterMark" || (prop \ "@name").text == "addTimestamp")
    } (prop \ "@name").text match {
      case "waterMark" => (prop \ "definition").foreach(kd => (kd \ "@name").text match {
        case "timeField" => this._wmTimeField = Some(kd.text)
        case "delayThreshold" => this._wmDelayThreshold = Some(kd.text)
        case _ =>
      })
      case "addTimestamp" => this._addTimestamp = Try(prop.text.toBoolean).getOrElse(false)
    }
  }

  /**
   * Specify the water-mark time field
   *
   * @param field
   * @return
   */
  def watermarkTimeField(field: String): DeltaStreamReader = { this._wmTimeField = Some(field); this }

  /**
   * Specify teh water-mark delay threshold
   *
   * @param duration
   * @return
   */
  def watermarkDelayThreshold(duration: String): DeltaStreamReader = { this._wmDelayThreshold = Some(duration); this }

  /**
   * Flag of whether or not to add __timestamp with current timestamp.
   *
   * @param value
   * @return
   */
  def addTimestamp(value: Boolean = false): DeltaStreamReader = { this._addTimestamp = value; this }
}
