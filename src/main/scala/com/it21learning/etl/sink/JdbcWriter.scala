package com.it21learning.etl.sink

import java.util.Properties
import com.it21learning.common.PropertyKey
import com.it21learning.common.io.FileChannel
import com.it21learning.etl.common.{Actor, ExecutionContext}
import com.it21learning.etl.sink.process.JdbcMicroBatchWriter
import org.apache.spark.sql.{DataFrame, SparkSession}
import scala.util.{Failure, Success, Try}
import com.typesafe.config.Config
import org.apache.spark.sql.execution.datasources.jdbc.JDBCOptions
import scala.xml.NodeSeq

/**
 * Write to RDBMS
 *
 * The following is one xml definition:
 *
 * <actor type="com.it21learning.etl.source.JdbcWriter">
 *   <property name="db">
 *     <definition name="driver">com.mysql.jdbc.Driver</definition>
 *     <definition name="url">jdbc:mysql://localhost:3306/events_db</definition>
 *     <definition name="dbName">train</definition>
 *     <definition name="tableName">train</definition>
 *     <definition name="user">it21</definition>
 *     <definition name="password">abc@xyz</definition>
 *   </property>
 *   <property name="dbOptions">
 *     <!-- number of partitions used to control the concurrency of writing -->
 *     <definition name="numPartitions">9</definition>
 *     <!-- number of rows for each batch when writing -->
 *     <definition name="batchSize">1600</definition>
 *     <!-- isolation level of transactions -->
 *     <definition name="isolationlevel">READ_UNCOMMITTED</definition>
 *   </property>
 *
 *   <!--
 *     the mode for writing with one of the values - overwrite, append and merge.
 *     when merge option is used, the sinkStatement is required.
 *   -->
 *   <property name="mode">overwrite|append|merge</property>
 *   <!-- The sink statement for combining new/updated data into target. It could an insert or merge statement -->
 *   <property name="sinkStatement">
 *     <!-- Only one of the following definitions is accepted. The first definition is picked if there are multiple ones -->
 *     <definition name="file">scripts/events.sql</definition>
 *     <!--
 *       The merge statement to make database change idempotent.
 *       IMPORTANT NOTE: In the example here, the columns (id, name, description, price, batch_id) must match the columns
 *       in the source data-frame and target database table.
 *     -->
 *     <definition name="string">
 *       merge into products as p
 *         using values(@id, @name, @description, @price, @batch_id) as v(id, name, description, price, batch_id)
 *           on p.id = v.id and p.batch_id = v.batch_id
 *         when matched then
 *           update set
 *             p.name = v.name,
 *             p.description = v.description,
 *             p.price = v.price
 *         when not matched then
 *           insert (id, name, description, price, batch_id) values(v.id, v.name, v.description, v.price, v.batch_id)
 *
 *       <!-- for mysql -->
 *       insert into products(id, name, description, price, batch_id) values(@id, @name, @description, @price, @batch_id)
 *         on duplicate key update set
 *           name = @name,
 *           description = @description,
 *           price = @price
 *     </definition>
 *   </property>
 *
 *   <property name="view">events</property>
 * </actor>
 */
final class JdbcWriter extends Actor {
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

  //the write mode
  @PropertyKey("mode", false)
  private var _mode: Option[String] = None
  //merge query
  private var _sinkStmt: Option[String] = None

  //the source view
  @PropertyKey("view", false)
  private var _sourceView: Option[String] = None

  /**
   * Run the jdbc-writer
   *
   * @param ctx - the execution context
   * @param session - the spark-session
   * @return
   */
  def run(ctx: ExecutionContext)(implicit session: SparkSession): Option[DataFrame] = for {
    _ <- validate(this._dbDriver, "The dbDriver in JdbcWriter is mandatory.")
    driver <- this._dbDriver
    _ <- validate(this._dbUrl, "The dbUrl in JdbcWriter is mandatory.")
    url <- this._dbUrl
    _ <- validate(this._dbTable, "The dbName in JdbcWriter is mandatory.")
    table <- this._dbTable
    _ <- validate(this._dbUser, "The dbUser in JdbcWriter is mandatory.")
    user <- this._dbUser
    _ <- validate(this._dbPassword, "The dbPassword in JdbcWriter is mandatory.")
    password <- this._dbPassword
    _ <- validate(this._mode, "The mode in JdbcWriter is mandatory or its value is invalid.", Seq("overwrite", "append", "merge"))
    mode <- this._mode
    _ <- validate(this._sourceView, "The view in JdbcWriter is mandatory.")
    df <- this._sourceView.flatMap(name => ctx.getView(name))
  } yield Try {
    //check numPartitions
    if (!this._dbOptions.contains("numPartitions")) {
      this.dbOption("numPartitions", ctx.appCtx.ioConnections.toString)
    }
    //check batch size
    if (!this._dbOptions.contains("batchSize")) {
      this.dbOption("batchSize", ctx.appCtx.ioBatchSize.toString)
    }
    mode match {
      case "merge" => {
        val initWriter = new JdbcMicroBatchWriter()
          .dbDriver(driver).dbUrl(url).dbTable(table).dbUser(user).dbPassword(password).dbOptions(this._dbOptions)
        val writer = this._sinkStmt match {
          case Some(stmt) => initWriter.sinkStatement(stmt)
          case _ => initWriter
        }
        writer.write(df, 0L)
      }
      case _ => {
        //properties for writing
        val props = new Properties()
        props.put("driver", driver)
        props.put("dbTable", table)
        props.put("user", user)
        props.put("password", password)
        this._dbOptions.filter { case(k, _) => k != "numPartitions" }.foreach { case(k, v) => props.put(k, v) }
        //write
        df.coalesce(this._dbOptions("numPartitions").toInt).write.mode(mode).jdbc(url, table, props)
      }
    }
  } match {
    case Success(_) => df
    case Failure(ex) => throw new RuntimeException(s"Cannot write to target - ${this._dbTable}.", ex)
  }

  /**
  * Initialize the jdbc writer from the xml definition
  *
  * @param config     - the configuration object
  * @param session    - the spark-session object
  */
  override def init(definition: NodeSeq, config: Config)(implicit session: SparkSession): Unit = {
    super.init(definition, config)

    //check the sql-statement
    (this.getProperty[String]("sinkStatement.string"), this.getProperty[String]("sinkStatement.file")) match {
      case (Some(ss), _) => this._sinkStmt = Some(ss)
      case (_, Some(fs)) => this._sinkStmt = Some(FileChannel.loadAsString(fs))
      case _ =>
    }
    validate(this._dbDriver, "The dbDriver in JdbcWriter is mandatory.")
    validate(this._dbUrl, "The dbUrl in JdbcWriter is mandatory.")
    validate(this._dbTable, "The dbTable in JdbcWriter is mandatory.")
    validate(this._dbUser, "The dbUser in JdbcWriter is mandatory.")
    validate(this._dbPassword, "The dbPassword in JdbcWriter is mandatory.")
    validate(this._mode, "The mode in JdbcWriter is mandatory or its value is invalid.", Seq("overwrite", "append", "merge"))
    validate(this._sourceView, "The view in JdbcWriter is mandatory.")
  }

  /**
   * The jdbc driver
   * @param driver
   * @return
   */
  def dbDriver(driver: String): JdbcWriter = { this._dbDriver = Some(driver); this }

  /**
   * The jdbc url
   * @param url
   * @return
   */
  def dbUrl(url: String): JdbcWriter = { this._dbUrl = Some(url); this }

  /**
   * The jdbc table
   * @param database
   * @return
   */
  def dbName(database: String): JdbcWriter = { this._dbName = Some(database); this }

  /**
   * The jdbc table
   * @param table
   * @return
   */
  def dbTable(table: String): JdbcWriter = { this._dbTable = Some(table); this }

  /**
   * The user for connection
   * @param user
   * @return
   */
  def dbUser(user: String): JdbcWriter = { this._dbUser = Some(user); this }

  /**
   * The user's password
   * @param password
   * @return
   */
  def dbPassword(password: String): JdbcWriter = { this._dbPassword = Some(password); this }

  /**
   * The load option
   *
   * @param name
   * @param value
   * @return
   */
  def dbOption(name: String, value: String): JdbcWriter = { this._dbOptions = this._dbOptions + (name -> value); this }
  /**
   * The load options
   *
   * @param opts
   * @return
   */
  def dbOptions(opts: Map[String, String]): JdbcWriter = { this._dbOptions = this._dbOptions ++ opts; this }

  /**
   * The write mode
   * @param mode
   * @return
   */
  def writeMode(mode: String): JdbcWriter = { this._mode = Some(mode); this }

  /**
   * The source view
   * @param view
   * @return
   */
  def sourceView(view: String): JdbcWriter = { this._sourceView = Some(view); this }
}
