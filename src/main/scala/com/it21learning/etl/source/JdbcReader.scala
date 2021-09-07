package com.it21learning.etl.source

import com.it21learning.common.PropertyKey
import com.it21learning.etl.common.{Actor, ExecutionContext}
import com.typesafe.config.Config
import org.apache.spark.sql.{DataFrame, SparkSession}
import scala.util.{Failure, Success, Try}
import scala.xml.NodeSeq

/**
 * Read from RDBMS
 *
 * The following is one xml definition:
 *
 * <actor type="com.it21learning.etl.source.JdbcReader">
 *   <property name="db">
 *     <definition name="driver">com.mysql.jdbc.Driver</definition>
 *     <definition name="url">jdbc:mysql://localhost:3306/events_db</definition>
 *     <definition name="dbName">train</definition>
 *     <definition name="tableName">train</definition>
 *     <definition name="user">it21</definition>
 *     <definition name="password">abc@xyz</definition>
 *   </property>
 *
 *   <property name="dbOptions">
 *     <!--
 *       The following 3 options must be all specified if one of them specified
 *       partitionColumn - partition the table when reading in parallel from multiple workers, must be a numeric, date or timestamp
 *       lowerBound & upperBound - decide the partition stride.
 *     -->
 *     <definition name="partitionColumn">user_id</definition>
 *     <definition name="lowerBound">10</definition>
 *     <definition name="upperBound">10000</definition>
 *     <!-- The maximum number of partitions that can be used for parallelism in table reading -->
 *     <definition name="numPartitions">10</definition>
 *     <!-- The JDBC fetch size in rows, which determines how many rows to fetch per round trip -->
 *     <definition name="fetchSize">1000</definition>
 *   </property>
 * </actor>
 */
final class JdbcReader extends Actor {
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

  /**
   * Initialize the jdbc reader from the xml definition
   *
   * @param config     - the configuration object
   * @param session    - the spark-session object
   */
  override def init(definition: NodeSeq, config: Config)(implicit session: SparkSession): Unit = {
    super.init(definition, config)

    validate(this._dbDriver, "The dbDriver in JdbcReader is mandatory.")
    validate(this._dbUrl, "The dbUrl in JdbcReader is mandatory.")
    validate(this._dbTable, "The dbTable in JdbcReader is mandatory.")
    validate(this._dbUser, "The dbUser in JdbcReader is mandatory.")
    validate(this._dbPassword, "The dbPassword in JdbcReader is mandatory.")
  }

  /**
   * Run the jdbc-reader
   *
   * @param ctx - the execution context
   * @param session - the spark-session
   * @return
   */
  def run(ctx: ExecutionContext)(implicit session: SparkSession): Option[DataFrame] = for {
    _ <- validate(this._dbDriver, "The dbDriver in JdbcReader is mandatory.")
    driver <- this._dbDriver
    _ <- validate(this._dbUrl, "The dbUrl in JdbcReader is mandatory.")
    url <- this._dbUrl
    _ <- validate(this._dbTable, "The dbTable in JdbcReader is mandatory.")
    table <- this._dbTable
    _ <- validate(this._dbUser, "The dbUser in JdbcReader is mandatory.")
    user <- this._dbUser
    _ <- validate(this._dbPassword, "The dbPassword in JdbcReader is mandatory.")
    password <- this._dbPassword
  } yield Try {
    this._dbOptions.foldLeft {
      session.read.format("jdbc")
        .option("driver", driver).option("url", url).option("dbTable", table).option("user", user).option("password", password)
    }((s, o) => s.option(o._1, o._2)).load
  } match {
    case Success(df) => df
    case Failure(ex) => throw new RuntimeException(s"Cannot load from source - ${this._dbTable}.", ex)
  }

  /**
   * The jdbc driver
   * @param driver
   * @return
   */
  def dbDriver(driver: String): JdbcReader = { this._dbDriver = Some(driver); this }

  /**
   * The jdbc url
   * @param url
   * @return
   */
  def dbUrl(url: String): JdbcReader = { this._dbUrl = Some(url); this }

  /**
   * The jdbc table
   * @param database
   * @return
   */
  def dbName(database: String): JdbcReader = { this._dbName = Some(database); this }

  /**
   * The jdbc table
   * @param table
   * @return
   */
  def dbTable(table: String): JdbcReader = { this._dbTable = Some(table); this }

  /**
   * The user for connection
   * @param user
   * @return
   */
  def dbUser(user: String): JdbcReader = { this._dbUser = Some(user); this }

  /**
   * The user's password
   * @param password
   * @return
   */
  def dbPassword(password: String): JdbcReader = { this._dbPassword = Some(password); this }

  /**
   * The load option
   *
   * @param name
   * @param value
   * @return
   */
  def dbOption(name: String, value: String): JdbcReader = { this._dbOptions = this._dbOptions + (name -> value); this }
  /**
   * The load options
   *
   * @param opts
   * @return
   */
  def dbOptions(opts: Map[String, String]): JdbcReader = { this._dbOptions = this._dbOptions ++ opts; this }
}
