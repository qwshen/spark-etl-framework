package com.it21learning.etl.sink.process

import java.sql._
import com.it21learning.common.logging.Loggable
import com.it21learning.etl.common.stream.ContinuousWriter
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._
import org.apache.spark.sql.execution.datasources.jdbc.JDBCOptions
import org.apache.spark.sql.jdbc.{JdbcDialect, JdbcDialects, JdbcType}
import scala.util.{Failure, Success, Try}

/**
 * To write batch rows into target database. This is normally used in spark micro-batch/continuous streaming.
 */
private[etl] final class JdbcContinuousWriter(options: Map[String, String], sinkStmt: Option[String]) extends ContinuousWriter with Loggable {
  //the connection object
  private var _connection: Option[Connection] = None
  //the db table
  private val _dbTable = options.get("dbtable")
  //flag of whether or not support transaction
  private var _supportTransactions: Boolean = false
  //the jdbc dialect
  private var _dialect: Option[JdbcDialect] = None

  //the variables defined in the sink-statement
  private var _parameters: Seq[(String, Int)] = Nil
  //refined sink-statement
  private var _statement: Option[String] = None

  //the current partition id
  private var _partitionId: Option[Long] = None
  //the current epoch id
  private var _epochId: Option[Long] = None

  /**
   * Open a connection for write for the implementation of ContinuousWriter
   *
   * @param partitionId
   * @param epochId
   * @return
   */
  def open(partitionId: Long, epochId: Long): Boolean = {
    //set
    this._partitionId = Some(partitionId)
    this._epochId = Some(epochId)

    val jdbcOptions: JDBCOptions = new JDBCOptions(this.options)
    this._connection = Some(DriverManager.getConnection(jdbcOptions.url, jdbcOptions.asConnectionProperties))
    this._dialect = Some(JdbcDialects.get(jdbcOptions.url))
    for {
      conn <- this._connection
      if jdbcOptions.isolationLevel != Connection.TRANSACTION_NONE
    } Try {
      val metadata = conn.getMetaData

      //calculate the isolation level
      val finalIsolationLevel = if (metadata.supportsTransactions()) {
        if (metadata.supportsTransactionIsolationLevel(jdbcOptions.isolationLevel)) jdbcOptions.isolationLevel else metadata.getDefaultTransactionIsolation
      }
      else {
        Connection.TRANSACTION_NONE
      }
      this._supportTransactions = finalIsolationLevel != Connection.TRANSACTION_NONE

      //apply isolation level
      if (this._supportTransactions) {
        conn.setAutoCommit(false)
        conn.setTransactionIsolation(finalIsolationLevel)
      }
    } match {
      case Success(_) =>
      case Failure(t) => this.logger.warn(t.getMessage)
    }
    true
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
    this._connection.foreach(c => Try { c.close() })
    if (errorOrNull != null) {
      throw errorOrNull
    }
    this._connection = None
  }

  //write the rows to target database
  def write(rows: Seq[Row], batchId: Option[Long]): Unit = for {
    schema <- rows.headOption.map(head => head.schema)
    sinkStmt <- {
      this.init(schema)
      this._statement
    }
    connection <- this._connection
    dialect <- this._dialect
  } {
    val stmt = connection.prepareStatement(sinkStmt)
    try {
      for (row <- rows) {
        this._parameters.foreach {
          case (param, index) => schema.fields.find(f => f.name == param) match {
            case Some(field) => field.dataType match {
              case StringType => Try(row.getAs[String](field.name)) match {
                case Success(v) => stmt.setString(index, v)
                case Failure(_) => stmt.setNull(index, java.sql.Types.VARCHAR)
              }
              case IntegerType => Try(row.getAs[Int](field.name)) match {
                case Success(v) => stmt.setInt(index, v)
                case _ => stmt.setNull(index, java.sql.Types.INTEGER)
              }
              case TimestampType => Try(row.getAs[java.sql.Timestamp](field.name)) match {
                case Success(v) => stmt.setTimestamp(index, v)
                case _ => stmt.setNull(index, java.sql.Types.TIMESTAMP)
              }
              case DateType => Try(row.getAs[java.sql.Date](field.name)) match {
                case Success(v) => stmt.setDate(index, v)
                case _ => stmt.setNull(index, java.sql.Types.DATE)
              }
              case t: DecimalType => Try(row.getDecimal(row.fieldIndex(field.name))) match {
                case Success(v) => stmt.setBigDecimal(index, v)
                case _ => stmt.setNull(index, java.sql.Types.DECIMAL)
              }
              case LongType => Try(row.getAs[Long](field.name)) match {
                case Success(v) => stmt.setLong(index, v)
                case _ => stmt.setNull(index, java.sql.Types.BIGINT)
              }
              case DoubleType => Try(row.getAs[Double](field.name)) match {
                case Success(v) => stmt.setDouble(index, v)
                case _ => stmt.setNull(index, java.sql.Types.DOUBLE)
              }
              case FloatType => Try(row.getAs[Float](field.name)) match {
                case Success(v) => stmt.setFloat(index, v)
                case _ => stmt.setNull(index, java.sql.Types.FLOAT)
              }
              case BooleanType => Try(row.getAs[Boolean](field.name)) match {
                case Success(v) => stmt.setBoolean(index, v)
                case _ => stmt.setNull(index, java.sql.Types.BOOLEAN)
              }
              case ArrayType(et, _) => Try {
                val typeName = dialect.getJDBCType(et)
                  .orElse {
                    et match {
                      case IntegerType => Option(JdbcType("INTEGER", java.sql.Types.INTEGER))
                      case LongType => Option(JdbcType("BIGINT", java.sql.Types.BIGINT))
                      case DoubleType => Option(JdbcType("DOUBLE PRECISION", java.sql.Types.DOUBLE))
                      case FloatType => Option(JdbcType("REAL", java.sql.Types.FLOAT))
                      case ShortType => Option(JdbcType("INTEGER", java.sql.Types.SMALLINT))
                      case ByteType => Option(JdbcType("BYTE", java.sql.Types.TINYINT))
                      case BooleanType => Option(JdbcType("BIT(1)", java.sql.Types.BIT))
                      case StringType => Option(JdbcType("TEXT", java.sql.Types.CLOB))
                      case BinaryType => Option(JdbcType("BLOB", java.sql.Types.BLOB))
                      case TimestampType => Option(JdbcType("TIMESTAMP", java.sql.Types.TIMESTAMP))
                      case DateType => Option(JdbcType("DATE", java.sql.Types.DATE))
                      case t: DecimalType => Option(JdbcType(s"DECIMAL(${t.precision},${t.scale})", java.sql.Types.DECIMAL))
                      case _ => None
                    }
                  }
                  .getOrElse {
                    throw new IllegalArgumentException(s"Can't get JDBC type for ${et.simpleString}.")
                  }
                  .databaseTypeDefinition.toLowerCase.split("\\(")(0)
                connection.createArrayOf(typeName, row.getSeq[AnyRef](row.fieldIndex(field.name)).toArray)
              } match {
                case Success(v) => stmt.setArray(index, v)
                case _ => stmt.setNull(index, java.sql.Types.ARRAY)
              }
              case ShortType => Try(row.getAs[Short](field.name)) match {
                case Success(v) => stmt.setShort(index, v)
                case _ => stmt.setNull(index, java.sql.Types.INTEGER)
              }
              case ByteType => Try(row.getAs[Byte](field.name)) match {
                case Success(v) => stmt.setByte(index, v)
                case _ => stmt.setNull(index, java.sql.Types.SMALLINT)
              }
              case BinaryType => Try(row.getAs[scala.Array[Byte]](field.name)) match {
                case Success(v) => stmt.setBytes(index, v)
                case _ => stmt.setNull(index, java.sql.Types.BINARY)
              }
              case _ => throw new RuntimeException("Unsupported data type in JdbcWriteProcessor.")
            }
            case _ => if (param == "batchId" && batchId.isDefined) {
              stmt.setLong(index, batchId.get)
            }
            else {
              throw new RuntimeException(s"The parameter [$param] referenced in the statement does not exist in the source data-frame.")
            }
          }
        }
        stmt.addBatch()
      }
      stmt.executeBatch()
      if (this._supportTransactions) {
        connection.commit()
      }
    }
    finally {
      stmt.close()
    }
  }

  //initialize the parameter list and sink-statement
  private def init(schema: StructType): Unit = for {
    table <- this._dbTable
    if (this._statement.isEmpty)
  } {
    val stmt = this.sinkStmt match {
      case Some(stmt) => stmt
      case _ => s"""insert into $table(${schema.fields.map(f => f.name).mkString(",")}) values(${schema.fields.map(f => "@" + f.name).mkString(",")})"""
    }
    val parameters = "@[a-z|_|A-Z]+[a-z|_|A-Z|0-9]?".r.findAllIn(stmt).toList.zipWithIndex
    this._parameters = parameters.map { case(k, v) => (k.stripPrefix("@"), v + 1) }
    this._statement = Some(parameters.map(p => p._1).foldLeft(stmt)((s, p) => s.replace(p, "?")))
  }
}
