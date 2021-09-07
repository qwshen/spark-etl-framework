package com.it21learning.etl.sink.common

import com.it21learning.common.PropertyKey
import com.it21learning.etl.common.{Actor, ExecutionContext}
import com.it21learning.etl.common.stream.{ContinuousWriter, MicroBatchWriter}
import com.typesafe.config.Config
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.{DataFrame, SparkSession}
import scala.util.{Failure, Success, Try}
import scala.xml.NodeSeq

/**
 * General Stream Writer which hosts a Micro-Batch or Continuous initWriter.
 *
 * The following is one xml definition:
 *
 * <actor type="com.it21learning.etl.sink.common.StreamWriter">
 *   <! --
 *     The following defines the micro-batch or continuous initWriter
 *     which must be inherited from MicroBatchWriter or ContinuousWriter respectively.
 *   -->
 *   <writer type="com.it21learning.etl.sink.process.JdbcContinuousWriter">
 *     <property name="db">
 *       <definition name="driver">com.mysql.jdbc.Driver</definition>
 *       <definition name="url">jdbc:mysql://localhost:3306/events_db</definition>
 *       <definition name="dbName">train</definition>
 *       <definition name="tableName">train</definition>
 *       <definition name="user">it21</definition>
 *       <definition name="password">abc@xyz</definition>
 *     </property>
 *     <!-- the properties for controlling the batch size & transaction when saving data to target database -->
 *     <property name="dbOptions">
 *       <!-- number of partitions -->
 *       <definition name="numPartitions>9</definition>
 *       <!-- number of rows for each batch -->
 *       <definition name="batchSize">1600</definition>
 *       <!-- isolation level of transactions -->
 *       <definition name="isolationlevel">READ_UNCOMMITTED</definition>
 *     </property>
 *     <!-- The sink statement for combining new/updated data into target. It could an insert or merge statement -->
 *     <property name="sinkStatement">
 *       <!--
 *         Only one of the following definitions is accepted. The first definition is picked if there are multiple ones
 *         If not specified, the insert statement will be constructed by the schema.
 *       -->
 *       <definition name="file">scripts/events.sql</definition>
 *       <!--
 *         The merge statement to make database change idempotent.
 *         IMPORTANT NOTE: In the example here, the columns (id, name, description, price, batch_id) must match the columns
 *         in the source data-frame and target database table.
 *       -->
 *       <definition name="string">
 *         merge into products as p
 *           using values(@id, @name, @description, @price, @batch_id) as v(id, name, description, price, batch_id)
 *             on p.id = v.id and p.batch_id = v.batch_id
 *           when matched then
 *             update set
 *               p.name = v.name,
 *               p.description = v.description,
 *               p.price = v.price
 *           when not matched then
 *             insert (id, name, description, price, batch_id) values(v.id, v.name, v.description, v.price, v.batch_id)
 *
 *         <!-- for mysql -->
 *         insert into products(id, name, description, price, batch_id) values(@id, @name, @description, @price, @batch_id)
 *           on duplicate key update set
 *             name = @name,
 *             description = @description,
 *             price = @price
 *       </definition>
 *     </property>
 *  </writer>
 *
 *   <!-- The options for writing to Kafka -->
 *   <property name="options">
 *     <definition name="checkpointLocation">/tmp/checkpoint-staging</definition>
 *   </property>
 *   <!-- The trigger defines how often to write to Kafka -->
 *   <property name="trigger">
 *     <definition name="mode">continuous|processingTime|once</definition>
 *     <definition name="interval">5 seconds</definition>
 *   </property>
 *
 *   <!-- The output mode defines how to output result - completely, incrementally. This is mandatory -->
 *   <property name="outputMode">complete|append|update</property>
 *   <!-- How long to wait for the streaming to execute before shut it down. This is only for test-debug purpose -->
 *   <property name="waitTimeInMs">16000</property>
 *
 *   <property name="view">events</property>
 * </actor>
 */
final class StreamWriter extends Actor {
  //the write type - either MicroBatchWriter or ContinuousWriter
  private var _writer: Option[Either[MicroBatchWriter, ContinuousWriter]] = None

  //the options for stream-writing
  @PropertyKey("options", true)
  private var _options: Map[String, String] = Map.empty[String, String]
  //trigger mode
  @PropertyKey("trigger.mode", false)
  private var _triggerMode: Option[String] = None
  //trigger interval
  @PropertyKey("trigger.interval", false)
  private var _triggerInterval: Option[String] = None

  //the output mode
  @PropertyKey("outputMode", false)
  private var _outputMode: Option[String] = None
  //wait time in ms for test
  @PropertyKey("waitTimeInMs", false)
  private var _waittimeInMs: Option[Long] = None

  //the source view
  @PropertyKey("view", false)
  private var _sourceView: Option[String] = None

  /**
   * Initialize the jdbc initWriter from the xml definition
   *
   * @param config     - the configuration object
   * @param session    - the spark-session object
   */
  override def init(definition: NodeSeq, config: Config)(implicit session: SparkSession): Unit = {
    super.init(definition, config)

    (definition \ "writer").headOption.foreach(w => {
      val writer = Try(Class.forName( (w \ "@type").text).newInstance) match {
        case Success(writer: MicroBatchWriter) => { this._writer = Some(Left(writer)); writer }
        case Success(writer: ContinuousWriter) => { this._writer = Some(Right(writer)); writer }
        case _ => throw new RuntimeException("The initWriter type is not valid. It must be either MicroBatchWriter or ContinuousWriter.")
      }
      writer.init(config)
      //writer.init(this.parse(w, Nil))
    })
    validate(this._writer, "The writer (either MicroBatchWriter or ContinuousWriter) is mandatory in StreamWriter.")
    validate(this._outputMode, "The mode in StreamWriter is mandatory or its value is invalid.", Seq("complete", "append", "update"))
    validate(this._sourceView, "The view in StreamWriter is mandatory.")
  }

  /**
   * Run the jdbc-stream-initWriter
   *
   * @param ctx - the execution context
   * @param session - the spark-session
   * @return
   */
  def run(ctx: ExecutionContext)(implicit session: SparkSession): Option[DataFrame] = for {
    _ <- validate(this._writer, "The writer (either MicroBatchWriter or ContinuousWriter) is mandatory in StreamWriter.")
    writer <- this._writer
    _ <- validate(this._outputMode, "The mode in JdbcStreamWriter is mandatory or its value is invalid.", Seq("complete", "append", "update"))
    mode <- this._outputMode
    _ <- validate(this._sourceView, "The view in JdbcStreamWriter is mandatory.")
    df <- this._sourceView.flatMap(name => ctx.getView(name))
  } yield Try {
    //the initial writer
    val initWriter = this._options.foldLeft(df.writeStream)((q, o) => q.option(o._1, o._2)).outputMode(mode)
    //query for the streaming write
    val queryWriter = writer match {
      case Right(cw) => {
        val bcWriter = session.sparkContext.broadcast(cw)
        initWriter.foreach(bcWriter.value)
      }
      case Left(mw) => initWriter.foreachBatch { (batchDf: DataFrame, batchId: Long) => mw.write(batchDf, batchId) }
    }
    val query = this._triggerMode match {
      case Some(md) if md == "continuous" => queryWriter.trigger(Trigger.Continuous(this._triggerInterval.getOrElse("0 ms")))
      case Some(md) if md == "processingTime" => queryWriter.trigger(Trigger.ProcessingTime(this._triggerInterval.getOrElse("0 ms")))
      case Some(md) if md == "once" => queryWriter.trigger(Trigger.Once())
      case _ => throw new RuntimeException("The writer and trigger mode doesn't match.")
    }
    this._waittimeInMs match {
      case Some(ts) => query.start.awaitTermination(ts)
      case _ => query.start.awaitTermination()
    }
  } match {
    case Success(_) => df
    case Failure(ex) => throw new RuntimeException(s"Cannot stream write to target in StreamWriter.", ex)
  }

  /**
   * Set the continuous writer
   * @param writer
   * @return
   */
  def continuousWriter(writer: ContinuousWriter): StreamWriter = { this._writer = Some(Right(writer)); this }

  /**
   * Set the continuous writer
   * @param writer
   * @return
   */
  def microbatchWriter(writer: MicroBatchWriter): StreamWriter = { this._writer = Some(Left(writer)); this }

  /**
   * The trigger mode
   * @param mode
   * @return
   */
  def triggerMode(mode: String): StreamWriter = { this._triggerMode = Some(mode); this }

  /**
   * The trigger interval
   * @param interval
   * @return
   */
  def triggerInterval(interval: String): StreamWriter = { this._triggerInterval = Some(interval); this }

  /**
   * The output mode
   * @param mode
   * @return
   */
  def outputMode(mode: String): StreamWriter = { this._outputMode = Some(mode); this }

  /**
   * The wait time in milli-second. This normally is used in tests.
   * @param tms
   * @return
   */
  def waitTimeinMs(tms: Long): StreamWriter = { this._waittimeInMs = Some(tms); this }

  /**
   * The source view
   * @param view
   * @return
   */
  def sourceView(view: String): StreamWriter = { this._sourceView = Some(view); this }
}
