package com.it21learning.etl.sink.process

import com.it21learning.common.PropertyKey
import com.it21learning.common.io.FileChannel
import com.it21learning.etl.common.stream.MicroBatchWriter
import org.apache.spark.sql.DataFrame
import scala.util.{Failure, Success, Try}

/**
 * To write batch rows into target database. This is normally used in spark micro-batch/continuous streaming.
 */
final class JdbcMicroBatchWriter extends MicroBatchWriter {
  //the jdbc driver
  @PropertyKey("db.driver", false)
  private var _dbDriver: Option[String] = None
  //the jdbc url
  @PropertyKey("db.url", false)
  private var _dbUrl: Option[String] = None
  //the target database
  @PropertyKey("db.dbName", false)
  private var _dbName: Option[String] = None
  //the target table
  @PropertyKey("db.tableName", false)
  private var _dbTable: Option[String] = None

  //the db user & password
  @PropertyKey("db.user", false)
  private var _dbUser: Option[String] = None
  @PropertyKey("db.password", false)
  private var _dbPassword: Option[String] = None

  //read options
  @PropertyKey("dbOptions", true)
  private var _dbOptions: Map[String, String] = Map.empty[String, String]

  //sink sql-statement
  private var _sinkStmt: Option[String] = None

  /**
   * Initialize the properties
   * @param properties - contains the values by property-keys
   *  @return - the properties (with their values) that are not applied.
   */
//  override def init(properties: Map[String, Any]): Map[String, Any] = {
//    val restProperties = super.init(properties)
//    validate(this._dbDriver, "The dbDriver in JdbcMicroBatchWriter is mandatory.")
//    validate(this._dbUrl, "The dbUrl in JdbcMicroBatchWriter is mandatory.")
//    validate(this._dbTable, "The dbTable in JdbcMicroBatchWriter is mandatory.")
//    validate(this._dbUser, "The dbUser in JdbcMicroBatchWriter is mandatory.")
//    validate(this._dbPassword, "The dbPassword in JdbcMicroBatchWriter is mandatory.")
//
//    //check the sql-statement
//    (restProperties.get("sinkStatement.string"), restProperties.get("sinkStatement.file")) match {
//      case (Some(ss: String), _) => this._sinkStmt = Some(ss)
//      case (_, Some(fs: String)) => this._sinkStmt = Some(FileChannel.loadAsString(fs))
//      case _ =>
//    }
//    restProperties
//  }

    /**
   * Write the batch data-frame for the implementation of BatchWriter
   *
   * @param batchDf
   * @param batchId
   */
  def write(batchDf: DataFrame, batchId: Long): Unit = for {
    _ <- validate(this._dbDriver, "The dbDriver in JdbcStreamWriter is mandatory.")
    driver <- this._dbDriver
    _ <- validate(this._dbUrl, "The dbUrl in JdbcStreamWriter is mandatory.")
    url <- this._dbUrl
    _ <- validate(this._dbTable, "The dbTable in JdbcStreamWriter is mandatory.")
    table <- this._dbTable
    _ <- validate(this._dbUser, "The dbUser in JdbcStreamWriter is mandatory.")
    user <- this._dbUser
    _ <- validate(this._dbPassword, "The dbPassword in JdbcStreamWriter is mandatory.")
    password <- this._dbPassword
  } {
    val numPartitions = Try(this._dbOptions("numPartitions").toInt) match {
      case Success(num) => num
      case Failure(t) => throw new RuntimeException("The numPartitions cannot be empty, it must be set as a position valid number.")
    }
    val batchSize = Try(this._dbOptions("batchSize").toInt) match {
      case Success(num) => num
      case Failure(t) => throw new RuntimeException("The batchSize cannot be empty, it must be set as a position valid number.")
    }

    val bcDriver = batchDf.sparkSession.sparkContext.broadcast(driver)
    val bcUrl = batchDf.sparkSession.sparkContext.broadcast(url)
    val bcTable = batchDf.sparkSession.sparkContext.broadcast(table)
    val bcUser = batchDf.sparkSession.sparkContext.broadcast(user)
    val bcPassword = batchDf.sparkSession.sparkContext.broadcast(password)
    val bcDbOptions = batchDf.sparkSession.sparkContext.broadcast(this._dbOptions)
    val bcSinkStmt = batchDf.sparkSession.sparkContext.broadcast(this._sinkStmt)
    val bcBatchId = batchDf.sparkSession.sparkContext.broadcast(batchId)
    //write the batch data-frame
    batchDf.coalesce(numPartitions).rdd.foreachPartition(rows => {
      //create the writer
      val initWriter = new JdbcContinuousWriter()
        .dbDriver(bcDriver.value).dbUrl(bcUrl.value).dbTable(bcTable.value).dbUser(bcUser.value).dbPassword(bcPassword.value)
        .dbOptions(bcDbOptions.value)
      val writer = bcSinkStmt.value match {
        case Some(stmt) => initWriter.sinkStatement(stmt)
        case _ => initWriter
      }
      //open connection
      writer.open(0L, 0L)
      Try {
        rows.grouped(batchSize).foreach(g => writer.write(g, Some(bcBatchId.value)))
      } match {
        case Success(_) => writer.close(null)
        case Failure(t) => writer.close(t)
      }
    })
  }

  /**
   * The jdbc driver
   * @param driver
   * @return
   */
  def dbDriver(driver: String): JdbcMicroBatchWriter = { this._dbDriver = Some(driver); this }

  /**
   * The jdbc url
   * @param url
   * @return
   */
  def dbUrl(url: String): JdbcMicroBatchWriter = { this._dbUrl = Some(url); this }

  /**
   * The jdbc table
   * @param database
   * @return
   */
  def dbName(database: String): JdbcMicroBatchWriter = { this._dbName = Some(database); this }

  /**
   * The jdbc table
   * @param table
   * @return
   */
  def dbTable(table: String): JdbcMicroBatchWriter = { this._dbTable = Some(table); this }

  /**
   * The user for connection
   * @param user
   * @return
   */
  def dbUser(user: String): JdbcMicroBatchWriter = { this._dbUser = Some(user); this }

  /**
   * The user's password
   * @param password
   * @return
   */
  def dbPassword(password: String): JdbcMicroBatchWriter = { this._dbPassword = Some(password); this }

  /**
   * The db-write option
   *
   * @param name
   * @param value
   * @return
   */
  def dbOption(name: String, value: String): JdbcMicroBatchWriter = { this._dbOptions = this._dbOptions + (name -> value); this }
  /**
   * The db-write options
   *
   * @param opts
   * @return
   */
  def dbOptions(opts: Map[String, String]): JdbcMicroBatchWriter = { this._dbOptions = this._dbOptions ++ opts; this }

  /**
   * Set the sink statement
   * @param sql
   * @return
   */
  def sinkStatement(sql: String): JdbcMicroBatchWriter = { this._sinkStmt = Some(sql); this }
}