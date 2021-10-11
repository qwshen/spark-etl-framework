package com.qwshen.etl.common

import com.qwshen.common.PropertyKey
import com.typesafe.config.Config
import org.apache.spark.sql.SparkSession

private [etl] abstract class HBaseActor[T] extends Actor { self: T =>
  //the connection properties for connecting to hbase
  @PropertyKey("connection.*", true)
  protected var _connectionProperties: Map[String, String] = Map.empty[String, String]
  //the hbase table including namespace
  @PropertyKey("table", true)
  protected var _table: Option[String] = None

  /**
   * Initialize the kafka reader
   */
  override def init(properties: Seq[(String, String)], config: Config)(implicit session: SparkSession): Unit = {
    super.init(properties, config)

    //check if driver & url are specified
    if (!this._connectionProperties.exists(_._1.equals("hbase-site.xml"))) {
      if (!Seq("hbase.zookeeper.quorum", "hbase.zookeeper.property.clientPort", "zookeeper.znode.parent").forall(k => this._connectionProperties.exists(_._1.equals(k)))) {
        throw new RuntimeException("The mandatory properties for connecting to HBase is not defined.")
      }
    }
  }

  /**
   * The connection property for connecting to HBase
   *
   * @param name
   * @param value
   * @return
   */
  def connectionProperty(name: String, value: String): T = { this._connectionProperties = this._connectionProperties + (name -> value); this }

  /**
   * The connection properties for connecting to HBase
   * @param props
   * @return
   */
  def connectionProperties(props: Map[String, String]): T = { this._connectionProperties = this._connectionProperties ++ props; this }

  /**
   * The table name in HBase
   *
   * @param tblName
   * @return
   */
  def tableName(tblName: String): T = { this._table = Some(tblName); this }
}
