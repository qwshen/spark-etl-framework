package com.it21learning.etl.sink

import com.it21learning.common.PropertyKey
import com.it21learning.etl.common.{Actor, ExecutionContext}
import com.typesafe.config.Config
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.util.{Failure, Success, Try}
import scala.xml.NodeSeq

/**
 * This writer is for writing csv, json, avro & parquet files to the target location
 *
 * The following is the definition in xml format
 *   <actor type="com.it21learning.etl.sink.FileStreamWriter">
 *     <!-- the format of the target files. This is mandatory -->
 *     <property name="format">csv</property>
 *     <!-- options for writing files -->
 *     <property name="options">
 *       <option name="header" value="true" />
 *       <option name="delimiter" value=";" />
 *       <option name="checkpointLocation" value="/mnt/checkpoint/events" />
 *     </property>
 *
 *     <!-- The trigger defines how often to write to Kafka -->
 *     <property name="trigger">
 *       <definition name="mode">continuous|processingTime|once</definition>
 *       <definition name="interval">5 seconds</definition>
 *     </property>
 *     <!-- The partitions to write into -->
 *     <property name="partitionBy">process_date</property>
 *
 *     <!-- The streaming outputMode - complete|update|append. This is mandatory -->
 *     <property name="outputMode">complete</property>
 *     <!-- How long to wait for the streaming to execute before shut it down. This is only for test-debug purpose -->
 *     <property name="waitTimeInMs">16000</property>
 *
 *     <!-- the target location. This is mandatory -->
 *     <property name="path">value1</property>
 *
 *     <!-- the source view for being written out. This is mandatory -->
 *     <property name="view">events</property>
 *   </actor>
 */
final class FileStreamWriter extends Actor {
  //the file format
  @PropertyKey("format", false)
  private var _format: Option[String] = None
  //the options for loading the file
  @PropertyKey("options", true)
  private var _options: Map[String, String] = Map.empty[String, String]
  //The columns separated by comma(,) used to partition data when writing.
  private var _partitionBy: Seq[String] = Nil
  //the file location
  @PropertyKey("path", false)
  private var _path: Option[String] = None

  //the view for writing out
  @PropertyKey("view", false)
  private var _view: Option[String] = None

  //output mode
  @PropertyKey("outputMode", false)
  private var _outputMode: Option[String] = None

  //trigger mode
  @PropertyKey("trigger.mode", false)
  private var _triggerMode: Option[String] = None
  //trigger interval
  @PropertyKey("trigger.interval", false)
  private var _triggerInterval: Option[String] = None

  //wait time in ms for test
  @PropertyKey("waitTimeInMs", false)
  private var _waittimeInMs: Option[Long] = None

  /**
   * Run the file-writer
   *
   * @param ctx - the execution context
   * @param session - the spark-session
   * @return
   */
  override def run(ctx: ExecutionContext)(implicit session: SparkSession): Option[DataFrame] = for {
    _ <- validate(this._format, "The format in FileStreamWriter is mandatory or its value is invalid.", Seq("csv", "json", "parquet", "avro", "orc"))
    fmt <- this._format
    _ <- validate(this._outputMode, "The output-mode in FileStreamWriter is mandatory or its value is invalid.", Seq("complete", "append", "update"))
    mode <- this._outputMode
    _ <- validate(this._path, "The path in FileStreamWriter is mandatory.")
    uri <- this._path
    _ <- validate(this._view, "The view in FileStreamWriter is mandatory.")
    df <- this._view.flatMap(name => ctx.getView(name))
  } yield Try {
    val initWriter = this._options.foldLeft(df.writeStream.format(fmt))((w, o) => w.option(o._1, o._2))
    //with partitionBy
    val partitionWriter = if (this._partitionBy.nonEmpty) initWriter.partitionBy(this._partitionBy: _*) else initWriter
    //combine with trigger
    val triggerWriter = (this._triggerMode, this._triggerInterval) match {
      case (Some(m), Some(t)) if (m == "continuous") => partitionWriter.trigger(Trigger.Continuous(t))
      case (Some(m), Some(t)) if (m == "processingTime") => partitionWriter.trigger(Trigger.ProcessingTime(t))
      case (Some(m), _) if (m == "once") => partitionWriter.trigger(Trigger.Once())
      case _ => partitionWriter
    }
    this._waittimeInMs match {
      case Some(ts) => triggerWriter.outputMode(mode).start(uri).awaitTermination(ts)
      case _ => triggerWriter.outputMode(mode).start(uri).awaitTermination()
    }
  } match {
    case Success(_) => df
    case Failure(ex) => throw new RuntimeException(s"Cannot write data to the target - $uri.", ex)
  }

  /**
   * Initialize the file reader from the xml definition
   *
   * @param config  - the configuration object
   * @param session - the spark-session object
   */
  override def init(definition: NodeSeq, config: Config)(implicit session: SparkSession): Unit = {
    super.init(definition, config)

    //partition by
    this._partitionBy = this.getProperty[String]("partitionBy").map(c => c.split(",")) match {
      case Some(cs) => cs
      case _ => Nil
    }
    validate(this._format, "The format in FileStreamWriter is mandatory or its value is invalid.", Seq("csv", "json", "parquet", "avro", "orc"))
    validate(this._outputMode, "The output-mode in FileStreamWriter is mandatory or its value is invalid.", Seq("complete", "append", "update"))
    validate(this._path, "The path in FileStreamWriter is mandatory.")
    validate(this._view, "The view in FileStreamWriter is mandatory.")
  }

  /**
   * The format of the source files.
   *
   * @param format
   * @return
   */
  def targetFormat(format: String): FileStreamWriter = { this._format = Some(format); this }

  /**
   * The load option
   *
   * @param name
   * @param value
   * @return
   */
  def writeOption(name: String, value: String): FileStreamWriter = { this._options = this._options + (name -> value); this }

  /**
   * The load options
   *
   * @param options
   * @return
   */
  def writeOptions(options: Map[String, String]): FileStreamWriter = { this._options = this._options ++ options; this }

  /**
   * Partition by
   *
   * @param column
   * @param columns
   * @return
   */
  def partitionBy(column: String, columns: String*): FileStreamWriter = { this._partitionBy = Seq(column) ++ columns.toSeq; this }

  /**
   * The source path of the files
   *
   * @param path
   * @return
   */
  def targetPath(path: String): FileStreamWriter = { this._path = Some(path); this }

  /**
   * The view of its data to be written out
   *
   * @param view
   * @return
   */
  def forView(view: String): FileStreamWriter = { this._view = Some(view); this }

  /**
   * The trigger mode
   *
   * @param mode
   * @return
   */
  def triggerMode(mode: String): FileStreamWriter = { this._triggerMode = Some(mode); this }

  /**
   * The trigger interval
   * @param duration
   * @return
   */
  def triggerInterval(duration: String): FileStreamWriter = { this._triggerInterval = Some(duration); this }

  /**
   * The output mode
   *
   * @param mode
   * @return
   */
  def outputMode(mode: String): FileStreamWriter = { this._outputMode = Some(mode); this }

  /**
   * Wait time for streaming to execute before shut it down
   *
   * @param waittime
   * @return
   */
  def waitTimeInMs(waittime: Long): FileStreamWriter = { this._waittimeInMs = Some(waittime); this }
}
