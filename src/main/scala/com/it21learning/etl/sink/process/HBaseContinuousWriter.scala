package com.it21learning.etl.sink.process

import com.it21learning.common.PropertyKey
import com.it21learning.common.io.HBaseChannel
import com.it21learning.etl.common.stream.ContinuousWriter
import org.apache.spark.sql.Row
import org.apache.hadoop.hbase.client.Connection
import org.apache.hadoop.security.UserGroupInformation
import org.apache.hadoop.security.token.Token
import org.apache.spark.SparkFiles
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import scala.util.{Failure, Success, Try}
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema

/**
 * Stream Write rows into HBase in continuous mode.
 */
final class HBaseContinuousWriter() extends ContinuousWriter {
  //the connection properties
  @PropertyKey("hbase", true)
  private var _hbaseProperties: Map[String, String] = Map.empty[String, String]
  //the hbase table
  private var _tblName: Option[String] = None
  //the key connector
  private var _keyConcatenator: Option[String] = None
  //the field mapping
  private var _fieldsMapping: Seq[(String, Option[String], Boolean)] = Nil

  //the connection object
  private var _connection: Option[Connection] = None
  //security token
  private var _securityToken: Option[String] = None

  /**
   * Initialize the properties
   * @param properties - contains the values by property-keys
   *  @return - the properties (with their values) that are not applied.
   */
  //override def init(properties: Map[String, Any]): Map[String, Any] = {
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
  //}

  /**
   * Open a connection for write for the implementation of ContinuousWriter
   * @param partitionId
   * @param epochId
   * @return
   */
  def open(partitionId : Long, epochId : Long): Boolean = {
    //set the security context
    this._securityToken.foreach(token => {
      val kerberosToken = new Token()
      kerberosToken.decodeFromUrlString(token)
      UserGroupInformation.getCurrentUser.addToken(kerberosToken)
    })

    val newProps = this._hbaseProperties.map(x => {
      if (x._1 == "hbase-site.xml" || x._1 == "core-site.xml" || x._1 == "hdfs-site.xml") {
        (x._1, SparkFiles.get(x._2))
      }
      else {
        (x._1, x._2)
      }
    })
    Try(HBaseChannel.open(newProps)) match {
      case Success(c) => this._connection = Some(c)
      case Failure(t) => throw t
    }
    this._connection.isDefined
  }

  /**
   * Write the current row for the implementation of ContinuousWriter
   * @param row
   */
  def process(row: Row): Unit = write(Seq(row), None)

  /**
   * Close the connection for the implemenation of ContinuousWriter
   * @param errorOrNull
   */
  def close(errorOrNull: Throwable) : Unit = {
    //close the connection
    for {
      connection <- this._connection
    } {
      Try {
        HBaseChannel.close(connection)
      } match {
        case _ =>
      }
    }
    //if error'ed, then error out
    if (errorOrNull != null) {
      throw errorOrNull
    }
    //reset
    this._connection = None
  }

  //write the rows to target database
  def write(rows: Seq[Row], batchId: Option[Long]): Unit = for {
    schema <- rows.headOption.map(_.schema)
    table <- this._tblName
    connection <- this._connection
  } {
    val newRows = this._fieldsMapping match {
      case Seq(_, _ @ _*) => {
        val keyColumns = this._fieldsMapping.filter(x => x._3).map(f => f._1)
        val keyConnector = this._keyConcatenator.getOrElse("&")
        val fldColumns = this._fieldsMapping.filter(_._2.isDefined).map(x => (x._1, x._2.get)).toMap

        val fields = schema.fields.map(field => (field.name, field)).toMap
        if (!fldColumns.map(x => fields.contains(x._1)).reduce((x, y) => x & y)) {
          throw new RuntimeException("At least one field referenced in the mapping does not exist in the data-frame.")
        }
        val newSchema = StructType(fldColumns.map(fc => StructField(fc._2, fields(fc._1).dataType, fields(fc._1).nullable)).toSeq :+ StructField(HBaseChannel.rowkeyName, StringType))
        rows.map(row => {
          val newValues = fldColumns.map(fc => row.get(row.fieldIndex(fc._1))).toArray :+ keyColumns.map(c => row.get(row.fieldIndex(c)).toString).mkString(keyConnector)
          new GenericRowWithSchema(newValues, newSchema)
        })
      }
      case _ => rows
    }
    HBaseChannel.write(newRows, table, connection)
  }

  /**
   * The hbase properties
   * @param props
   * @return
   */
  def properties(props: Map[String, String]): HBaseContinuousWriter = { this._hbaseProperties = props; this}

  /**
   * the hbase table
   * @param name
   * @return
   */
  def table(name: String): HBaseContinuousWriter = { this._tblName = Some(name); this }

  /**
   * the key connector
   * @param concatenator
   * @return
   */
  def keyConcatenator(concatenator: String): HBaseContinuousWriter = { this._keyConcatenator = Some(concatenator); this }

  /**
   * the field mapping
   * @param mapping
   * @return
   */
  def fieldsMapping(mapping: Seq[(String, Option[String], Boolean)]): HBaseContinuousWriter = { this._fieldsMapping = mapping; this }

  /**
   * The security token for connecting to target HBase
   * @param token
   * @return
   */
  def securityToken(token: String): HBaseContinuousWriter = { this._securityToken = Some(token); this }
}
