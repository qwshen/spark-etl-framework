package com.qwshen.common.io

import java.util
import org.apache.spark.sql.functions.col
import org.apache.hadoop.conf.Configuration
import com.qwshen.common.logging.Loggable
import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}
import org.apache.hadoop.hbase.client.{Connection, ConnectionFactory, IsolationLevel, Put, Result, Scan}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.protobuf.ProtobufUtil
import org.apache.hadoop.hbase.util.{Base64, Bytes}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.types._
import scala.collection.breakOut
import scala.util.{Failure, Success, Try}
import java.sql.Timestamp

/**
 * Implements the basic IO for HBase.
 */
object HBaseChannel extends Loggable {
  /**
   * The name of the row key in dataframe
   */
  val rowkeyName: String = "__:rowKey"

  /**
   * Open a HBase connection
   *
   * @param props
   * @return
   */
  def open(props: Map[String, String]): Connection = ConnectionFactory.createConnection(connectionConfiguration(props))

  /**
   * Read a table from HBase
   *
   * @param tblName
   * @param props
   * @param dbOptions
   * @param fieldsMap
   * @param session
   * @return
   */
  def read(connProps: Map[String, String], table: String)
    (fieldsMap: Map[String, String], options: Map[String, String], filter: Map[String, String])(implicit session: SparkSession): DataFrame = {
    //key & field columns
    val keyColumn: Option[String] = fieldsMap.find { case(_, from) => from.equals(HBaseChannel.rowkeyName) }.map(x => x._1)
    val fieldColumns: Seq[(String, String, String)] = fieldsMap.filter { case(_, from) => !from.equals(HBaseChannel.rowkeyName) }
      .map(kv => (kv._1, kv._2.split(":", -1).map(_.trim))).map(kv => (kv._1, kv._2(0), kv._2(1)))(breakOut)

    val scan = new Scan()
    //set the columns to be scanned
    fieldColumns.foreach { x => scan.addColumn(Bytes.toBytes(x._2), Bytes.toBytes(x._3)) }

    //key-start
    filter.get("keyStart").foreach(keyStart => scan.withStartRow(Bytes.toBytes(keyStart)))
    //key-stop
    filter.get("keyStop").foreach(keyStop => scan.withStopRow(Bytes.toBytes(keyStop)))
    //key-prefix
    filter.get("keyPrefix").foreach(keyPrefix => scan.setRowPrefixFilter(Bytes.toBytes(keyPrefix)))
    //start & end time-stamp
    (filter.get("tsStart"), filter.get("tsEnd")) match {
      case (Some(tsStart), Some(tsEnd)) => (Try(Timestamp.valueOf(tsStart)).toOption, Try(Timestamp.valueOf(tsEnd)).toOption) match {
        case (Some(dtStart), Some(dtEnd)) => scan.setTimeRange(dtStart.getTime, dtEnd.getTime)
        case _ =>
      }
      case _ =>
    }

    //isolation level
    options.get("isolationLevel").foreach {
      case "READ_COMMITTED" => scan.setIsolationLevel(IsolationLevel.READ_COMMITTED)
      case "READ_UNCOMMITTED" => scan.setIsolationLevel(IsolationLevel.READ_UNCOMMITTED)
      case _ =>
    }
    //max result sie
    options.get("maxResultSize").foreach(size => scan.setMaxResultSize(Try(size.toLong).getOrElse(-1)))
    //batch size
    options.get("batchSize").foreach(size => Try(size.toInt).toOption match {
      case Some(batchSize) => scan.setBatch(batchSize)
      case _ =>
    })

    val config = connectionConfiguration(connProps)
    config.set(TableInputFormat.INPUT_TABLE, table)
    config.set(TableInputFormat.SCAN, convertScanToString(scan))
    import session.implicits._
    val dfHBase = session.sparkContext.newAPIHadoopRDD(config, classOf[TableInputFormat], classOf[ImmutableBytesWritable], classOf[Result])
      .map(r => (Bytes.toString(r._2.getRow), fieldColumns.map(fc => Bytes.toString(r._2.getValue(Bytes.toBytes(fc._2), Bytes.toBytes(fc._3))))))
      .toDF("key", "value")
    val df = options.get("numPartitions") match {
      case Some(n) => Try(n.toInt) match {
        case Success(numPartitions) => dfHBase.repartition(numPartitions)
        case _ => dfHBase
      }
      case _ => dfHBase
    }
    keyColumn match {
      case Some(kc) => df.withColumnRenamed("key", kc)
        .select(fieldColumns.map(x => x._1).zipWithIndex.map { case(fc, idx) => 'value.getItem(idx).as(fc) } :+ col(kc): _*)
      case _ =>df.select(fieldColumns.map(x => x._1).zipWithIndex.map { case(fc, idx) => 'value.getItem(idx).as(fc) }: _*)
    }
  }

  /**
   * Write the rows to target table
   *
   * @param rows - the data to be written
   * @param tblName - the table name in HBase
   * @param connection - the connection object to HBase
   */
  def write(rows: Seq[Row], tblName: String, connection: Connection): Unit = for {
    schema <- rows.headOption.map(_.schema)
  } {
    val tbl = connection.getTable(TableName.valueOf(tblName))
    try {
      val puts = new util.ArrayList[Put]()
      for (row <- rows) {
        val values: Seq[(String, Array[Byte])] = schema.fields.map(field => Try {
          field.dataType match {
            case StringType => (field.name, Try(Bytes.toBytes(row.getAs[String](field.name))).getOrElse(Array.empty[Byte]))
            case IntegerType => (field.name, Try(Bytes.toBytes(row.getAs[Int](field.name).toString)).getOrElse(Array.empty[Byte]))
            case TimestampType => (field.name, Try(Bytes.toBytes(row.getAs[java.sql.Timestamp](field.name).toString)).getOrElse(Array.empty[Byte]))
            case DateType => (field.name, Try(Bytes.toBytes(row.getAs[java.sql.Date](field.name).toString)).getOrElse(Array.empty[Byte]))
            case t: DecimalType => (field.name, Try(Bytes.toBytes(row.getDecimal(row.fieldIndex(field.name)).toString)).getOrElse(Array.empty[Byte]))
            case LongType => (field.name, Try(Bytes.toBytes(row.getAs[Long](field.name).toString)).getOrElse(Array.empty[Byte]))
            case DoubleType => (field.name, Try(Bytes.toBytes(row.getAs[Double](field.name).toString)).getOrElse(Array.empty[Byte]))
            case FloatType => (field.name, Try(Bytes.toBytes(row.getAs[Float](field.name).toString)).getOrElse(Array.empty[Byte]))
            case BooleanType => (field.name, Try(Bytes.toBytes(row.getAs[Boolean](field.name).toString)).getOrElse(Array.empty[Byte]))
            case ShortType => (field.name, Try(Bytes.toBytes(row.getAs[Short](field.name).toString)).getOrElse(Array.empty[Byte]))
            case ByteType => (field.name, Try(Bytes.toBytes(row.getAs[Byte](field.name).toString)).getOrElse(Array.empty[Byte]))
            case BinaryType => (field.name, Try(row.getAs[scala.Array[Byte]](field.name)).getOrElse(Array.empty[Byte]))
            case _ => throw new RuntimeException("Unsupported data type in HBaseContinuousWriter.")
          }
        } match {
          case Success(v) => v
          case Failure(t) => {
            this.logger.warn(t.getMessage + t.getStackTrace.mkString(scala.util.Properties.lineSeparator))
            (field.name, Array.empty[Byte])
          }
        })
        val rowKey = values.find(_._1 == this.rowkeyName).map(_._2)
        if (rowKey.isEmpty) {
          throw new RuntimeException("The row-key column is not defined.")
        }
        val put = new Put(rowKey.get)
        values.filter(v => v._1 != this.rowkeyName && v._2.nonEmpty)
          .map(x => (x._1.split(":").map(_.trim), x._2))
          .foreach(x => put.addColumn(Bytes.toBytes(x._1(0)), Bytes.toBytes(x._1(1)), x._2))
        puts.add(put)
      }
      if (puts.size() > 0) {
        tbl.put(puts)
      }
    }
    finally {
      tbl.close()
    }
  }

  /**
   * Truncate the target table
   *
   * @param tblName
   * @param conn
   */
  def truncate(tblName: String, conn: Connection): Unit = {
    val admin = conn.getAdmin
    try {
      val tbl = TableName.valueOf(tblName)
      //truncate the table only if it is disabled.
      if (admin.isTableEnabled(tbl)) {
        admin.disableTable(tbl)
      }
      //truncate the table automatically enable the target table
      admin.truncateTable(tbl, true)
    }
    finally {
      admin.close()
    }
  }

  //prepare the connection configuration
  private def connectionConfiguration(props: Map[String, String]): Configuration = {
    val connKeys = scala.collection.mutable.Map[String, Boolean]()
    if (props.contains("hbase-site.xml")) {
      connKeys.put("hbase-site.xml", true)
      if (props.contains("core-site.xml")) {
        connKeys.put("core-site.xml", true)
      }
      if (props.contains("hdfs-site.xml")) {
        connKeys.put("hdfs-site.xml", true)
      }
    }
    else {
      props.keys.foreach(key => connKeys.put(key, false))
    }
    if (connKeys.isEmpty) {
      throw new RuntimeException("The required properties for connecting HBase are missing.")
    }

    connKeys.foldLeft(HBaseConfiguration.create())((cfg, kv) => {
      if (kv._2)
        cfg.addResource(props(kv._1))
      else
        cfg.set(kv._1, props(kv._1))
      cfg
    })
  }

  //convert a scan to bytes
  private def convertScanToString(scan: Scan) = Base64.encodeBytes(ProtobufUtil.toScan(scan).toByteArray)

  /**
   * Close a connection
   *
   * @param conn
   */
  def close(conn: Connection): Unit = Try {
    conn.close()
  } match {
    case Success(_) =>
    case Failure(t) => this.logger.warn(t.getMessage)
  }
}
