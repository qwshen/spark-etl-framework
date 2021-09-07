//package com.it21learning.etl.sink
//
//import com.it21learning.common.PropertyKey
//import com.it21learning.etl.common.{Actor, ExecutionContext, HBaseActor}
//import com.it21learning.etl.sink.process.{HBaseContinuousWriter, HBaseMicroBatchWriter}
//import com.typesafe.config.Config
//import org.apache.spark.sql.{DataFrame, SparkSession}
//import scala.util.{Failure, Success, Try}
//import scala.xml.NodeSeq
//
///**
// * To write data-frame to HBase
// *
// * The following is the definition in xml format
// * <actor type="com.it21learning.etl.sink.HBaseWriter">
// *   <property name="hbase">
// *     <!--
// *       The following properties is for connecting to HBase.
// *       If hbase_site.xml is configured, then all other hbase.* will be ignored.
// *     -->
// *     <definition name="hbase.zookeeper.quorum">127.0.0.1</definition>
// *     <definition name="hbase.zookeeper.property.clientPort">2181</definition>
// *     <definition name="zookeeper.znode.parent">/hbase-unsecure</definition>
// *     <!-- the following two options are mandatory in secured hbase environment -->
// *     <definition name="hadoop.security.authentication">kerberos</definition>
// *     <definition name="hbase.security.authentication">kerberos</definition>
// *     <definition name="core-site.xml">/usr/hdp/current/hbase-server/conf/core-site.xml</definition>
// *     <definition name="hdfs-site.xml">/usr/hdp/current/hbase-server/conf/hdfs-site.xml</definition>
// *     <definition name="hbase-site.xml">/usr/hdp/current/hbase-server/conf/hbase-site.xml</definition>
// *   </property>
// *   <property name="data">
// *     <!--
// *       The name of HBase table.
// *       The keyConcatenator is used to concatenating all key columns if there are multiple ones. The default value is &.
// *     -->
// *     <table name="events_db:users" keyConcatenator="-">
// *       <field name="user_id" key="true" />
// *       <field name="user_name" to="profile:user_name" />
// *       <field name="address_street" to="location:address_street" />
// *     </table>
// *   </property>
// *   <property name="dbOptions">
// *     <option name="numPartitions" value="16" />
// *     <option name="batchSize" value="1600" />
// *   </property>
// *   <property name="options">
// *     <option name="checkpointLocation" value="/tmp/checkpoint-staging" />
// *   </property>
// *   <!-- The trigger defines how often to write to Kafka -->
// *   <property name="trigger">
// *     <definition name="mode">continuous|processingTime|once</definition>
// *     <definition name="interval">5 seconds</definition>
// *   </property>
// *   <!-- The output mode defines how to output result - completely, incrementally. This is mandatory -->
// *   <property name="outputMode">complete|append|update</property>
// *   <!-- How long to wait for the streaming to execute before shut it down. This is only for test-debug purpose -->
// *   <property name="waitTimeInMs">16000</property>
// *   <property name="view">events</property>
// * </actor>
// */
//final class HBaseStreamWriter extends Actor with HBaseActor {
//  //the connection properties
//  @PropertyKey("hbase", true)
//  private var _hbaseProperties: Map[String, String] = Map.empty[String, String]
//
//  //the hbase table
//  private var _tblName: Option[String] = None
//  //the key connector
//  private var _keyConcatenator: Option[String] = None
//  //the field mapping
//  private var _fieldMapping: Seq[(String, Option[String], Boolean)] = Nil
//
//  //the options for loading the file
//  @PropertyKey("dbOptions", true)
//  private var _dbOptions: Map[String, String] = Map.empty[String, String]
//
//  //the options for stream-writing
//  @PropertyKey("options", true)
//  private var _options: Map[String, String] = Map.empty[String, String]
//  //trigger mode
//  @PropertyKey("trigger.mode", false)
//  private var _triggerMode: Option[String] = None
//  //trigger interval
//  @PropertyKey("trigger.interval", false)
//  private var _triggerInterval: Option[String] = None
//
//  //the output mode
//  @PropertyKey("outputMode", false)
//  private var _outputMode: Option[String] = None
//  //wait time in ms for test
//  @PropertyKey("waitTimeInMs", false)
//  private var _waittimeInMs: Option[Long] = None
//
//  //the view for writing out
//  @PropertyKey("view", false)
//  protected var _view: Option[String] = None
//
//  /**
//   * Run the jdbc-writer
//   *
//   * @param ctx - the execution context
//   * @param session - the spark-session
//   * @return
//   */
//  def run(ctx: ExecutionContext)(implicit session: SparkSession): Option[DataFrame] = for {
//    _ <- validate(this._tblName, "The table name in HBaseStreamWriter is mandatory.")
//    tbl <- this._tblName
//    _ <- validate(this._outputMode, "The mode in HBaseStreamWriter is mandatory or its value is invalid.", Seq("complete", "append", "update"))
//    mode <- this._outputMode
//    _ <- validate(this._view, "The source-view in HBaseStreamWriter is mandatory.")
//    df <- this._view.flatMap(name => ctx.getView(name))
//  } yield Try {
//    val dfHBase = mapToFamilyColumn(df, this._fieldMapping, this._keyConcatenator)
//    //the initial writer
//    val writer = this._options.foldLeft(dfHBase.writeStream)((q, o) => q.option(o._1, o._2)).outputMode(mode)
//
//    //query for the streaming write
//    val query = this._triggerMode match {
//      case Some(m) if (m == "continuous") => {
//        val props = session.sparkContext.broadcast(this._hbaseProperties)
//        val table = session.sparkContext.broadcast(tbl)
//        writer.foreach(new HBaseContinuousWriter(props.value, table.value))
//      }
//      case _ => {
//        val numPartitions = this._dbOptions.getOrElse("numPartitions", ctx.appCtx.ioConnections.toString).toInt
//        val batchSize = this._dbOptions.getOrElse("batchSize", ctx.appCtx.ioBatchSize.toString).toInt
//        val batchWriter = new HBaseMicroBatchWriter(this._hbaseProperties, tbl, numPartitions, batchSize)
//        writer.foreachBatch { (batchDf: DataFrame, batchId: Long) => batchWriter.write(batchDf, batchId) }
//      }
//    }
//    this._waittimeInMs match {
//      case Some(ts) => query.start.awaitTermination(ts)
//      case _ => query.start.awaitTermination()
//    }
//  } match {
//    case Success(_) => df
//    case Failure(ex) => throw new RuntimeException(s"Cannot write to target - ${this._tblName}.", ex)
//  }
//
//  /**
//   * Initialize the file reader from the xml definition
//   *
//   * @param config     - the configuration object
//   * @param session    - the spark-session object
//   */
//  override def init(definition: NodeSeq, config: Config)(implicit session: SparkSession): Unit = {
//    super.init(definition, config)
//
//    for (xmlDefinition <- this.getProperty[String]("data")) {
//      val dataDefinition = scala.xml.XML.loadString(xmlDefinition)
//      (dataDefinition \ "table").headOption.foreach(tbl =>  {
//        this._tblName = Some((tbl \ "@name").text)
//        this._keyConcatenator = (tbl \ "@keyConcatenator").headOption.map(_.text)
//        this._fieldMapping = (tbl \ "field").map(fld => ((fld \"@name").text, (fld \ "@to").headOption.map(_.text), Try((fld \ "@key").text.toBoolean).getOrElse(false)))
//      })
//    }
//
//    if (!this._hbaseProperties.contains("hbase-site.xml") && !this._hbaseProperties.contains("hbase.zookeeper.quorum")) {
//      throw new RuntimeException("The connection properties in HBaseReader is not setup properly.")
//    }
//    else if (this._fieldMapping.isEmpty) {
//      throw new RuntimeException("There is no field-mapping for how to save fields in data-frame to target HBase table.")
//    }
//    validate(this._tblName, "The table name in HBaseWriter is mandatory.")
//    validate(this._outputMode, "The mode in JdbcStreamWriter is mandatory or its value is invalid.", Seq("complete", "append", "update"))
//    validate(this._view, "The source-view in HBaseWriter is mandatory.")
//  }
//
//  /**
//   * The connection property for connecting to HBase
//   *
//   * @param name
//   * @param value
//   * @return
//   */
//  def connectionProperty(name: String, value: String): HBaseStreamWriter = { this._hbaseProperties = this._hbaseProperties + (name -> value); this }
//
//  /**
//   * The connection properties for connecting to HBase
//   * @param props
//   * @return
//   */
//  def connectionProperties(props: Map[String, String]): HBaseStreamWriter = { this._hbaseProperties = this._hbaseProperties ++ props; this }
//
//  /**
//   * The table name in HBase
//   *
//   * @param tblName
//   * @return
//   */
//  def tableName(tblName: String): HBaseStreamWriter = { this._tblName = Some(tblName); this }
//
//  /**
//   * The key-concatenator for combining multiple key-columns
//   *
//   * @param value
//   * @return
//   */
//  def keyConcatenator(value: String): HBaseStreamWriter = { this._keyConcatenator = Some(value); this }
//
//  /**
//   * The data field mapping from data-frame to target HBase table.
//   *
//   * @param srcName
//   * @param dstName
//   * @param isKey
//   * @return
//   */
//  def dataField(srcName: String, dstName: String, isKey: Boolean): HBaseStreamWriter = { this._fieldMapping = this._fieldMapping :+ (srcName, Some(dstName), isKey); this }
//
//  /**
//   * The load option
//   *
//   * @param name
//   * @param value
//   * @return
//   */
//  def dbOption(name: String, value: String): HBaseStreamWriter = { this._dbOptions = this._dbOptions + (name -> value); this }
//  /**
//   * The load options
//   *
//   * @param opts
//   * @return
//   */
//  def dbOptions(opts: Map[String, String]): HBaseStreamWriter = { this._dbOptions = this._dbOptions ++ opts; this }
//
//  /**
//   * The load option
//   *
//   * @param name
//   * @param value
//   * @return
//   */
//  def option(name: String, value: String): HBaseStreamWriter = { this._options = this._options + (name -> value); this }
//  /**
//   * The load options
//   *
//   * @param opts
//   * @return
//   */
//  def options(opts: Map[String, String]): HBaseStreamWriter = { this._options = this._options ++ opts; this }
//
//  /**
//   * The trigger mode
//   *
//   * @param mode
//   * @return
//   */
//  def triggerMode(mode: String): HBaseStreamWriter = { this._triggerMode = Some(mode); this }
//
//  /**
//   * The trigger interval
//   * @param duration
//   * @return
//   */
//  def triggerInterval(duration: String): HBaseStreamWriter = { this._triggerInterval = Some(duration); this }
//
//  /**
//   * The output mode
//   *
//   * @param mode
//   * @return
//   */
//  def outputMode(mode: String): HBaseStreamWriter = { this._outputMode = Some(mode); this }
//
//  /**
//   * Wait time for streaming to execute before shut it down
//   *
//   * @param waittime
//   * @return
//   */
//  def waitTimeInMs(waittime: Long): HBaseStreamWriter = { this._waittimeInMs = Some(waittime); this }
//
//  /**
//   * The source view for writing into HBase
//   *
//   * @param view
//   * @return
//   */
//  def sourceView(view: String): HBaseStreamWriter = { this._view = Some(view); this }
//}
