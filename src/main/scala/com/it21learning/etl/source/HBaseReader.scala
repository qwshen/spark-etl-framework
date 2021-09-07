package com.it21learning.etl.source

import com.it21learning.common.PropertyKey
import com.it21learning.common.io.HBaseChannel
import com.it21learning.etl.common.{Actor, ExecutionContext}
import com.typesafe.config.Config
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
 *     <table name="events_db:users">
 *       <!-- the following user_id is from the row-key of each record -->
 *       <field name="user_id" from="__:rowKey" />
 *       <field name="user_name" from="profile:user_name" />
 *       <field name="address_street" from="location:address_street" />
 *     </table>
 *   </property>
 *   <property name="options">
 *     <definition name="keyStart">234256</definition>
 *     <definition name="keyStop">234256</definition>
 *     <definition name="keyPrefix">234</definition>
 *     <!-- Isolcation level. Either READ_COMMITTED or READ_UNCOMMITED -->
 *     <definition name="isolationLevel">READ_COMMITTED</definition>
 *     <!-- the max size of result size in bytes. -1 - unlimited -->
 *     <definition name="maxResultSize">-1</definition>
 *     <definition name="numPartitions">16</definition>
 *     <definition name="batchSize">1600</definition>
 *   </property>
 * </actor>
 */
final class HBaseReader extends Actor {
  //the connection properties
  @PropertyKey("hbase", true)
  private var _hbaseProperties: Map[String, String] = Map.empty[String, String]

  //the hbase table
  private var _tblName: Option[String] = None
  //the field mapping
  private var _fieldMapping: Seq[(String, String)] = Nil

  //the options for loading the file
  @PropertyKey("options", true)
  private var _options: Map[String, String] = Map.empty[String, String]

  /**
   * Run the hbase-reader
   *
   * @param ctx - the execution context
   * @param session - the spark-session
   * @return
   */
  def run(ctx: ExecutionContext)(implicit session: SparkSession): Option[DataFrame] = for {
    _ <- validate(this._tblName, "The table name in HBaseWriter is mandatory.")
    tbl <- this._tblName
  } yield Try {
    HBaseChannel.read(tbl, this._fieldMapping)(this._hbaseProperties, this._options)
  } match {
    case Success(df) => df
    case Failure(ex) => throw new RuntimeException(s"Cannot read from source - ${this._tblName}.", ex)
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
        this._fieldMapping = (tbl \ "field").map(fld => ((fld \"@name").text, (fld \ "@from").text))
      })
    }

    if (!this._hbaseProperties.contains("hbase-site.xml") && !this._hbaseProperties.contains("hbase.zookeeper.quorum")) {
      throw new RuntimeException("The connection properties in HBaseReader is not setup properly.")
    }
    if (this._fieldMapping.isEmpty) {
      throw new RuntimeException("There is no field-mapping for how to save fields in data-frame to target HBase table.")
    }
    validate(this._tblName, "The table name in HBaseReader is mandatory.")
  }

  /**
   * The connection property for connecting to HBase
   *
   * @param name
   * @param value
   * @return
   */
  def connectionProperty(name: String, value: String): HBaseReader = { this._hbaseProperties = this._hbaseProperties + (name -> value); this }

  /**
   * The connection properties for connecting to HBase
   * @param props
   * @return
   */
  def connectionProperties(props: Map[String, String]): HBaseReader = { this._hbaseProperties = this._hbaseProperties ++ props; this }

  /**
   * The table name in HBase
   *
   * @param tblName
   * @return
   */
  def tableName(tblName: String): HBaseReader = { this._tblName = Some(tblName); this }

  /**
   * The data field mapping from data-frame to target HBase table.
   *
   * @param srcName
   * @param dstName
   * @return
   */
  def dataField(srcName: String, dstName: String): HBaseReader = { this._fieldMapping = this._fieldMapping :+ (dstName, srcName); this }

  /**
   * The load option
   *
   * @param name
   * @param value
   * @return
   */
  def option(name: String, value: String): HBaseReader = { this._options = this._options + (name -> value); this }
  /**
   * The load options
   *
   * @param opts
   * @return
   */
  def option(opts: Map[String, String]): HBaseReader = { this._options = this._options ++ opts; this }
}
