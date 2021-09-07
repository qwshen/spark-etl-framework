package com.it21learning.etl.sink

import com.it21learning.common.PropertyKey
import com.it21learning.common.io.HBaseChannel
import com.it21learning.etl.common.{Actor, ExecutionContext}
import com.it21learning.etl.sink.process.HBaseMicroBatchWriter
import com.typesafe.config.Config
import org.apache.hadoop.hbase.security.token.TokenUtil
import org.apache.spark.sql.{DataFrame, SparkSession}
import scala.util.{Failure, Success, Try}
import scala.xml.NodeSeq

/**
 * To write data-frame to HBase
 *
 * The following is the definition in xml format
 * <actor type="com.it21learning.etl.sink.HBaseWriter">
 *   <property name="hbase">
 *     <!--
 *       The following properties is for connecting to HBase.
 *       If hbase_site.xml is configured, then all other hbase.* will be ignored.
 *     -->
 *     <definition name="hbase.zookeeper.quorum">127.0.0.1</definition>
 *     <definition name="hbase.zookeeper.property.clientPort">2181</definition>
 *     <definition name="zookeeper.znode.parent">/hbase-unsecure</definition>
 *     <!-- the following two options are mandatory in secured hbase environment -->
 *     <definition name="hadoop.security.authentication">kerberos</definition>
 *     <definition name="hbase.security.authentication">kerberos</definition>
 *     <definition name="core-site.xml">/usr/hdp/current/hbase-server/conf/core-site.xml</definition>
 *     <definition name="hdfs-site.xml">/usr/hdp/current/hbase-server/conf/hdfs-site.xml</definition>
 *     <definition name="hbase-site.xml">/usr/hdp/current/hbase-server/conf/hbase-site.xml</definition>
 *   </property>
 *   <property name="data">
 *     <!--
 *       The name of HBase table.
 *       The keyConcatenator is used to concatenating all key columns if there are multiple ones. The default value is &.
 *     -->
 *     <table name="events_db:users" keyConcatenator="-">
 *       <field name="user_id" key="true" />
 *       <field name="user_name" to="profile:user_name" />
 *       <field name="address_street" to="location:address_street" />
 *     </table>
 *   </property>
 *   <property name="dbOptions">
 *     <option name="numPartitions" value="16" />
 *     <option name="batchSize" value="1600" />
 *   </property>
 *   <!--
 *     The mode for writing to HBase
 *     - overwrite - the target table will be cleaned out then write the new data
 *     - merge -> the new data will be merged into target table
 *   -->
 *   <property name="mode">overwrite|merge</property>
 *   <property name="view">events</property>
 * </actor>
 */
final class HBaseWriter extends Actor {
  //the connection properties
  @PropertyKey("hbase", true)
  private var _hbaseProperties: Map[String, String] = Map.empty[String, String]

  //the hbase table
  private var _tblName: Option[String] = None
  //the key connector
  private var _keyConcatenator: Option[String] = None
  //the field mapping
  private var _fieldsMapping: Seq[(String, Option[String], Boolean)] = Nil

  //the options for loading the file
  @PropertyKey("dbOptions", true)
  private var _dbOptions: Map[String, String] = Map.empty[String, String]
  @PropertyKey("mode", false)
  //the mode for writing to HBase
  private var _mode: Option[String] = None

  //the view for writing out
  @PropertyKey("view", false)
  private var _view: Option[String] = None

  /**
   * Run the hbase-writer
   *
   * @param ctx - the execution context
   * @param session - the spark-session
   * @return
   */
  def run(ctx: ExecutionContext)(implicit session: SparkSession): Option[DataFrame] = for {
    _ <- validate(this._tblName, "The table name in HBaseWriter is mandatory.")
    tbl <- this._tblName
    _ <- validate(this._mode, "The mode in HBaseWriter is mandatory.", Seq("overwrite", "merge"))
    mode <- this._mode
    _ <- validate(this._view, "The source-view in HBaseWriter is mandatory.")
    df <- this._view.flatMap(name => ctx.getView(name))
  } yield Try {
    if (!this._dbOptions.contains("numPartitions")) {
      this.dbOption("numPartitions", ctx.appCtx.ioConnections.toString)
    }
    if (!this._dbOptions.contains("batchSize")) {
      this.dbOption("batchSize", ctx.appCtx.ioBatchSize.toString)
    }

    var kerberosToken: Option[String] = None
    val connection = HBaseChannel.open(this._hbaseProperties)
    try {
      //obtain the kerberos token
      val token = TokenUtil.obtainToken(connection)
      kerberosToken = Some(token.encodeToUrlString())

      //if overwrite, truncate the target table first
      if (mode == "overwrite") {
          HBaseChannel.truncate(tbl, connection)
      }
    }
    finally {
      HBaseChannel.close(connection)
    }

    val initWriter = kerberosToken.foldLeft(
      new HBaseMicroBatchWriter().properties(this._hbaseProperties).table(tbl).dbOptions(this._dbOptions)
    )((w, k) => w.securityToken(k))
    val writer = (this._keyConcatenator, this._fieldsMapping) match {
      case (Some(c), Seq(_, _ @ _*)) => initWriter.keyConcatenator(c).fieldsMapping(this._fieldsMapping)
      case (_, Seq(_, _ @ _*)) => initWriter.fieldsMapping(this._fieldsMapping)
      case (Some(c), _) => initWriter.keyConcatenator(c)
      case _ => initWriter
    }
    writer.write(df, 0L)
  } match {
    case Success(_) => df
    case Failure(ex) => throw new RuntimeException(s"Cannot write to target - ${this._tblName}.", ex)
  }

  /**
   * Initialize the file reader from the xml definition
   *
   * @param config     - the configuration object
   * @param session    - the spark-session object
   */
  override def init(definition: NodeSeq, config: Config)(implicit session: SparkSession): Unit = {
    super.init(definition, config)

    for (xmlDefinition <- this.getProperty[String]("data")) {
      val dataDefinition = scala.xml.XML.loadString(xmlDefinition)
      (dataDefinition \ "table").headOption.foreach(tbl =>  {
        this._tblName = Some((tbl \ "@name").text)
        this._keyConcatenator = (tbl \ "@keyConcatenator").headOption.map(_.text)
        this._fieldsMapping = (tbl \ "field").map(fld => ((fld \"@name").text, (fld \ "@to").headOption.map(_.text), Try((fld \ "@key").text.toBoolean).getOrElse(false)))
      })
    }

    if (!this._hbaseProperties.contains("hbase-site.xml") && !this._hbaseProperties.contains("hbase.zookeeper.quorum")) {
      throw new RuntimeException("The connection properties in HBaseReader is not setup properly.")
    }
    else if (this._fieldsMapping.isEmpty) {
      throw new RuntimeException("There is no field-mapping for how to save fields in data-frame to target HBase table.")
    }
    validate(this._tblName, "The table name in HBaseWriter is mandatory.")
    validate(this._mode, "The mode in HBaseWriter is mandatory.", Seq("overwrite", "merge"))
    validate(this._view, "The source-view in HBaseWriter is mandatory.")
  }

  /**
   * The connection property for connecting to HBase
   *
   * @param name
   * @param value
   * @return
   */
  def connectionProperty(name: String, value: String): HBaseWriter = { this._hbaseProperties = this._hbaseProperties + (name -> value); this }

  /**
   * The connection properties for connecting to HBase
   * @param props
   * @return
   */
  def connectionProperties(props: Map[String, String]): HBaseWriter = { this._hbaseProperties = this._hbaseProperties ++ props; this }

  /**
   * The table name in HBase
   *
   * @param tblName
   * @return
   */
  def tableName(tblName: String): HBaseWriter = { this._tblName = Some(tblName); this }

  /**
   * The key-concatenator for combining multiple key-columns
   *
   * @param value
   * @return
   */
  def keyConcatenator(value: String): HBaseWriter = { this._keyConcatenator = Some(value); this }

  /**
   * The data field mapping from data-frame to target HBase table.
   *
   * @param srcName
   * @param dstName
   * @param isKey
   * @return
   */
  def dataField(srcName: String, dstName: String, isKey: Boolean): HBaseWriter = { this._fieldsMapping = this._fieldsMapping :+ (srcName, Some(dstName), isKey); this }

  /**
   * The load option
   *
   * @param name
   * @param value
   * @return
   */
  def dbOption(name: String, value: String): HBaseWriter = { this._dbOptions = this._dbOptions + (name -> value); this }
  /**
   * The load options
   *
   * @param opts
   * @return
   */
  def dbOptions(opts: Map[String, String]): HBaseWriter = { this._dbOptions = this._dbOptions ++ opts; this }

  /**
   * The write mode - must be one of overwrite, merge.
   *
   * @param mode
   * @return
   */
  def writeMode(mode: String): HBaseWriter = { this._mode = Some(mode); this }

  /**
   * The source view for writing into HBase
   *
   * @param view
   * @return
   */
  def sourceView(view: String): HBaseWriter = { this._view = Some(view); this }
}
