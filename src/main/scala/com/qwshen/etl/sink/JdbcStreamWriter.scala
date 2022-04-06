package com.qwshen.etl.sink

import com.qwshen.common.{PropertyKey, VariableResolver}
import com.qwshen.etl.common.{ExecutionContext, JdbcActor}
import com.qwshen.etl.sink.process.{JdbcContinuousWriter, JdbcMicroBatchWriter}
import com.qwshen.common.io.FileChannel
import com.typesafe.config.Config
import org.apache.spark.sql.{DataFrame, SparkSession}
import scala.util.{Failure, Success, Try}

/**
 * Stream data to RDBMS
 */
class JdbcStreamWriter extends JdbcActor[JdbcStreamWriter] with VariableResolver {
  //merge query
  @PropertyKey("sink.sqlString", false)
  protected var _sinkSqlString: Option[String] = None
  @PropertyKey("sink.sqlFile", false)
  protected var _sinkSqlFile: Option[String] = None

  //trigger mode
  @PropertyKey("trigger.mode", true)
  protected var _triggerMode: Option[String] = None
  //trigger interval
  @PropertyKey("trigger.interval", true)
  protected var _triggerInterval: Option[String] = None

  //the output mode
  @PropertyKey("outputMode", true)
  protected var _outputMode: Option[String] = None
  //wait time in ms for test
  @PropertyKey("test.waittimeMS", false)
  protected var _waittimeInMs: Option[Long] = None

  //the source view
  @PropertyKey("view", true)
  protected var _sourceView: Option[String] = None

  /**
   * Initialize the kafka reader
   */
  override def init(properties: Seq[(String, String)], config: Config)(implicit session: SparkSession): Unit = {
    super.init(properties, config)

    validate(this._outputMode, "The output-mode for JdbcStreamWriter is invalid or not defined.", Seq("complete", "append", "update"))

    (this._sinkSqlString, this._sinkSqlFile) match {
      case (None, Some(_)) => this._sinkSqlString = this._sinkSqlFile.map(file => this.resolve(FileChannel.loadAsString(file))(config))
      case _ => this._sinkSqlString = this._sinkSqlString.map(stmt => this.resolve(stmt)(config))
    }
    validate(this._sinkSqlString, "The sink statement is required for JdbcStreamWriter.")
  }

  /**
   * Run the jdbc-stream-writer
   *
   * @param ctx - the execution context
   * @param session - the spark-session
   * @return
   */
  def run(ctx: ExecutionContext)(implicit session: SparkSession): Option[DataFrame] = for {
    mode <- this._outputMode
    df <- this._sourceView.flatMap(name => ctx.getView(name))
  } yield Try {
    val options = this._connection ++ this._options
    //the initial writer
    val writer = options.foldLeft(df.writeStream)((q, o) => q.option(o._1, o._2)).outputMode(mode)

    val opts = session.sparkContext.broadcast(options)
    val stmt = session.sparkContext.broadcast(this._sinkSqlString)
    //query for the streaming write
    val query = this._triggerMode match {
      case Some(m) if (m == "continuous") => writer.foreach(new JdbcContinuousWriter(opts.value, stmt.value))
      case _ =>
        val batchWriter = new JdbcMicroBatchWriter(opts.value, stmt.value)
        writer.foreachBatch { (batchDf: DataFrame, batchId: Long) => batchWriter.write(batchDf, batchId) }
    }
    this._waittimeInMs match {
      case Some(ts) => query.start.awaitTermination(ts)
      case _ => query.start.awaitTermination()
    }

  } match {
    case Success(_) => df
    case Failure(ex) => throw new RuntimeException(s"Cannot write to target - ${this._connection.get("dbtable")}.", ex)
  }

  /**
   * The sink-sql-statement for merging data into target
   * @param stmt
   * @return
   */
  def sinkSqlString(stmt: String): JdbcStreamWriter = { this._sinkSqlString = Some(stmt); this }
  /**
   * The sink-sql-file for merging data into target
   * @param file
   * @return
   */
  def sinkSqlFile(file: String): JdbcStreamWriter = { this._sinkSqlFile = Some(file); this }

  /**
   * The trigger mode
   *
   * @param mode
   * @return
   */
  def triggerMode(mode: String): JdbcStreamWriter = { this._triggerMode = Some(mode); this }

  /**
   * The trigger interval
   * @param duration
   * @return
   */
  def triggerInterval(duration: String): JdbcStreamWriter = { this._triggerInterval = Some(duration); this }

  /**
   * The output mode
   *
   * @param mode
   * @return
   */
  def outputMode(mode: String): JdbcStreamWriter = { this._outputMode = Some(mode); this }

  /**
   * Wait time for streaming to execute before shut it down
   *
   * @param waittime
   * @return
   */
  def waitTimeInMs(waittime: Long): JdbcStreamWriter = { this._waittimeInMs = Some(waittime); this }

  /**
   * The source view
   * @param view
   * @return
   */
  def sourceView(view: String): JdbcStreamWriter = { this._sourceView = Some(view); this }
}
