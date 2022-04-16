package com.qwshen.etl.sink

import com.qwshen.common.PropertyKey
import com.qwshen.etl.common.{ExecutionContext, IcebergActor}
import com.typesafe.config.Config
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.{DataFrame, SparkSession}
import scala.util.{Failure, Success, Try}

/**
 * This class is to stream data into an icerberg table.
 */
class IcebergStreamWriter extends IcebergActor[IcebergStreamWriter] {
  //output mode
  @PropertyKey("outputMode", true)
  protected var _outputMode: Option[String] = None

  //trigger mode
  @PropertyKey("trigger.mode", true)
  protected var _triggerMode: Option[String] = None
  //trigger interval
  @PropertyKey("trigger.interval", true)
  protected var _triggerInterval: Option[String] = None

  //wait time in ms for test
  @PropertyKey("test.waittimeMS", false)
  protected var _waittimeInMs: Option[Long] = None

  //the view to be written
  @PropertyKey("view", true)
  protected var _view: Option[String] = None

  /**
   * Initialize the actor with the properties & config
   */
  override def init(properties: Seq[(String, String)], config: Config)(implicit session: SparkSession): Unit = {
    super.init(properties, config)

    //the output-mode must be one of complete, append
    this.validate(this._outputMode, "The output-mode in IcebergStreamWriter must be either complete or append.", Seq("complete", "append"))
    //the output-mode must be one of complete, append
    this.validate(this._triggerMode, "The trigger-mode in IcebergStreamWriter must be either processingTime or once.", Seq("processingTime", "once"))
  }

  /**
   * Run the actor
   * @param ctx     - the execution context
   * @param session - the spark-session
   * @return
   */
  def run(ctx: ExecutionContext)(implicit session: SparkSession): Option[DataFrame] = for {
    table <- this._table
    mode <- this._outputMode
    df <- this._view.flatMap(name => ctx.getView(name))
  } yield Try {
    val initQuery = this._options.foldLeft(df.writeStream.format("iceberg").option("path", table))((w, o) => w.option(o._1, o._2)).outputMode(mode)

    val triggerQuery = (this._triggerMode, this._triggerInterval) match {
      case (Some(m), Some(t)) if (m == "processingTime") => initQuery.trigger(Trigger.ProcessingTime(t))
      case (Some(m), _) if (m == "once") => initQuery.trigger(Trigger.Once())
      case (Some(m), Some(t)) if (m == "continuous") => throw new RuntimeException("The iceberg steam-writer doesn't support continuous processing.")
      case _ => throw new RuntimeException("The trigger mode in Iceberg Stream Writer is invalid.")
    }

    this._waittimeInMs match {
      case Some(ts) => triggerQuery.start().awaitTermination(ts)
      case _ => triggerQuery.start().awaitTermination()
    }
  } match {
    case Success(_) => df
    case Failure(ex) => throw new RuntimeException(s"Cannot write data to the target - $table.", ex)
  }

  /**
   * Define the output-mode. The valid values are: complete, append, update
   *
   * @param value
   * @return
   */
  def outputMode(value: String): IcebergStreamWriter = { this._outputMode = Some(value); this }

  /**
   * The trigger mode
   *
   * @param mode
   * @return
   */
  def triggerMode(mode: String): IcebergStreamWriter = { this._triggerMode = Some(mode); this }

  /**
   * The trigger interval
   * @param duration
   * @return
   */
  def triggerInterval(duration: String): IcebergStreamWriter = { this._triggerInterval = Some(duration); this }

  /**
   * Wait time for streaming to execute before shut it down
   *
   * @param waittime
   * @return
   */
  def waitTimeInMs(waittime: Long): IcebergStreamWriter = { this._waittimeInMs = Some(waittime); this }

  /**
   * The name of the view to be written
   *
   * @param value
   * @return
   */
  def view(value: String): IcebergStreamWriter = { this._view = Some(value); this }
}
