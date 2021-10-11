package com.qwshen.etl.sink.process

import com.qwshen.common.io.HBaseChannel
import com.qwshen.etl.common.stream.ContinuousWriter
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
private[etl] class HBaseContinuousWriter(connectionProperties: Map[String, String], table: String, keyColumns: Seq[String], keyConcatenator: Option[String], fieldsMapping: Map[String, String], securityToken: Option[String]) extends ContinuousWriter {
  //the connection object to HBase
  private var _connection: Option[Connection] = None

  /**
   * Open a connection for write for the implementation of ContinuousWriter
   * @param partitionId
   * @param epochId
   * @return
   */
  def open(partitionId : Long, epochId : Long): Boolean = {
    //set the security context
    securityToken.foreach(token => {
      val kerberosToken = new Token()
      kerberosToken.decodeFromUrlString(token)
      UserGroupInformation.getCurrentUser.addToken(kerberosToken)
    })

    val newProps = connectionProperties.map(x => {
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
    connection <- this._connection
  } {
    val keyConnector = keyConcatenator.getOrElse("&")
    val fields = schema.fields.map(field => (field.name, field)).toMap
    if (!fieldsMapping.map(x => fields.contains(x._1)).reduce((x, y) => x & y)) {
      throw new RuntimeException("At least one field referenced in the mapping does not exist in the data-frame.")
    }
    val newSchema = StructType(fieldsMapping.map(fc => StructField(fc._2, fields(fc._1).dataType, fields(fc._1).nullable)).toSeq :+ StructField(HBaseChannel.rowkeyName, StringType))
    val newRows = rows.map(row => {
      val newValues = fieldsMapping.map(fc => row.get(row.fieldIndex(fc._1))).toArray :+ keyColumns.map(c => row.get(row.fieldIndex(c)).toString).mkString(keyConnector)
      new GenericRowWithSchema(newValues, newSchema)
    })
    HBaseChannel.write(newRows, table, connection)
  }
}
