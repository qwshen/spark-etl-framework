package com.it21learning.etl.sink.process

import com.it21learning.etl.common.stream.MicroBatchWriter
import org.apache.spark.sql.DataFrame

import java.io.File
import com.it21learning.common.{PropertyInitializer, PropertyKey, PropertyValidater}

import scala.util.{Failure, Success, Try}

/**
 * Write the rows into HBase.
 */
private[etl] final class HBaseMicroBatchWriter() extends MicroBatchWriter with PropertyInitializer with PropertyValidater {
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

  private var _securityToken: Option[String] = None

  /**
   * Initialize the properties
   * @param properties - contains the values by property-keys
   *  @return - the properties (with their values) that are not applied.
   */
//  override def init(properties: Map[String, Any]): Map[String, Any] = {
//    val restProperties = super.init(properties)
//    restProperties.get("data") match {
//      case Some(data: String) => {
//        val dataDefinition = scala.xml.XML.loadString(data)
//        (dataDefinition \ "table").headOption.foreach(tbl => {
//          this._tblName = Some((tbl \ "@name").text)
//          this._keyConcatenator = (tbl \ "@keyConcatenator").headOption.map(_.text)
//          this._fieldsMapping = (tbl \ "field").map(fld => ((fld \ "@name").text, (fld \ "@to").headOption.map(_.text), Try((fld \ "@key").text.toBoolean).getOrElse(false)))
//        })
//      }
//      case _ =>
//    }
//    if (!this._hbaseProperties.contains("hbase-site.xml") && !this._hbaseProperties.contains("hbase.zookeeper.quorum")) {
//      throw new RuntimeException("The connection properties in HBaseReader is not setup properly.")
//    }
//    else if (this._fieldsMapping.isEmpty) {
//      throw new RuntimeException("There is no field-mapping for how to save fields in data-frame to target HBase table.")
//    }
//    validate(this._tblName, "The table name in HBaseContinuousWriter is mandatory.")
//    restProperties
//  }

  /**
   * Write the batch data-frame for the implementation of BatchWriter
   *
   * @param batchDf
   * @param batchId
   */
  def write(batchDf: DataFrame, batchId: Long): Unit = for {
    _ <- validate(this._tblName, "The table name in HBaseContinuousWriter is mandatory.")
    tblName <- this._tblName
  } {
    val numPartitions = Try(this._dbOptions("numPartitions").toInt) match {
      case Success(num) => num
      case Failure(t) => throw new RuntimeException("The numPartitions cannot be empty, it must be set as a position valid number.")
    }
    val batchSize = Try(this._dbOptions("batchSize").toInt) match {
      case Success(num) => num
      case Failure(t) => throw new RuntimeException("The batchSize cannot be empty, it must be set as a position valid number.")
    }

    val newProps = this._hbaseProperties.map(x => {
      if (x._1 == "hbase-site.xml" || x._1 == "core-site.xml" || x._1 == "hdfs-site.xml") {
        batchDf.sparkSession.sparkContext.addFile(new File(x._2).getName)
        (x._1, x._1)
      }
      else {
        (x._1, x._2)
      }
    })
    val bcProperties = batchDf.sparkSession.sparkContext.broadcast(newProps)
    val bcTblName = batchDf.sparkSession.sparkContext.broadcast(tblName)
    val bcKeyConcatenator = batchDf.sparkSession.sparkContext.broadcast(this._keyConcatenator)
    val bcFieldsMapping = batchDf.sparkSession.sparkContext.broadcast(this._fieldsMapping)
    val bcSecurityToke = batchDf.sparkSession.sparkContext.broadcast(this._securityToken)
    //write the batch data-frame
    batchDf.coalesce(numPartitions).rdd.foreachPartition(rows => {
      //create the writer
      val initWriter = this._securityToken.foldLeft(
        new HBaseContinuousWriter().properties(bcProperties.value).table(bcTblName.value)
      )((w, k) => w.securityToken(k))
      val writer = (bcKeyConcatenator.value, bcFieldsMapping.value) match {
        case (Some(c), Seq(_, _ @ _*)) => initWriter.keyConcatenator(c).fieldsMapping(bcFieldsMapping.value)
        case (Some(c), _) => initWriter.keyConcatenator(c)
        case (_, Seq(_, _ @ _*)) => initWriter.fieldsMapping(bcFieldsMapping.value)
        case _ => initWriter
      }

      //open connection
      writer.open(0L, 0L)
      Try {
        rows.grouped(batchSize).foreach(g => writer.write(g, None))
      } match {
        case Success(_) => writer.close(null)
        case Failure(t) => writer.close(t)
      }
    })
  }

  /**
   * The hbase properties
   * @param props
   * @return
   */
  def properties(props: Map[String, String]): HBaseMicroBatchWriter = { this._hbaseProperties = props; this}

  /**
   * the hbase table
   * @param name
   * @return
   */
  def table(name: String): HBaseMicroBatchWriter = { this._tblName = Some(name); this }

  /**
   * the key connector
   * @param concatenator
   * @return
   */
  def keyConcatenator(concatenator: String): HBaseMicroBatchWriter = { this._keyConcatenator = Some(concatenator); this }

  /**
   * the field mapping
   * @param mapping
   * @return
   */
  def fieldsMapping(mapping: Seq[(String, Option[String], Boolean)]): HBaseMicroBatchWriter = { this._fieldsMapping = mapping; this }

  /**
   * Set the dbOptions
   * @param options
   * @return
   */
  def dbOptions(options: Map[String, String]): HBaseMicroBatchWriter = { this._dbOptions = options; this }

  /**
   * The security token for connecting to target HBase
   * @param token
   * @return
   */
  def securityToken(token: String): HBaseMicroBatchWriter = { this._securityToken = Some(token); this }
}
