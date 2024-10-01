package com.qwshen.etl.sink

import com.qwshen.common.{PropertyKey, VariableResolver}
import com.qwshen.common.io.FileChannel
import com.qwshen.etl.common.{JobContext, JdbcActor}
import com.qwshen.etl.sink.process.JdbcMicroBatchWriter
import org.apache.spark.sql.{DataFrame, SparkSession}
import scala.util.{Failure, Success, Try}
import com.typesafe.config.Config

/**
 * Write to RDBMS
 */
class JdbcWriter extends JdbcActor[JdbcWriter] with VariableResolver {
  //the write mode
  @PropertyKey("mode", true)
  protected var _mode: Option[String] = None

  //merge query
  @PropertyKey("sink.sqlString", false)
  protected var _sinkSqlString: Option[String] = None
  @PropertyKey("sink.sqlFile", false)
  protected var _sinkSqlFile: Option[String] = None

  //the source view
  @PropertyKey("view", true)
  protected var _sourceView: Option[String] = None

  /**
   * Run the jdbc-writer
   *
   * @param ctx - the execution context
   * @param session - the spark-session
   * @return
   */
  def run(ctx: JobContext)(implicit session: SparkSession): Option[DataFrame] = for {
    mode <- this._mode
    df <- this._sourceView.flatMap(name => ctx.getView(name))
  } yield Try {
    //check numPartitions
    if (!this._options.contains("numPartitions") || Try(this._options("numPartitions").toInt).isFailure) {
      this.dbOption("numPartitions", ctx.appCtx.ioConnections.toString)
    }
    //check batch size
    if (!this._options.contains("batchSize") || Try(this._options("batchSize").toInt).isFailure) {
      this.dbOption("batchSize", ctx.appCtx.ioBatchSize.toString)
    }
    mode match {
      case "merge" => new JdbcMicroBatchWriter(this._connection ++ this._options, this._sinkSqlString).write(df, 0L)
      case _ => for (numPartitions <- this._options.get("numPartitions").map(_.toInt)) {
        //write
        import com.qwshen.etl.utils.DataframeHelper._
        df.split(numPartitions).foreach(x => x.write.format("jdbc").options(this._connection ++ this._options).mode(mode).save())
      }
    }
  } match {
    case Success(_) => df
    case Failure(ex) => throw new RuntimeException(s"Cannot write to target - ${this._connection.get("dbtable")}.", ex)
  }

  /**
   * Initialize the kafka reader
   */
  override def init(properties: Seq[(String, String)], config: Config)(implicit session: SparkSession): Unit = {
    super.init(properties, config)

    this._mode match {
      case Some(s) if s.equals("merge") =>
        (this._sinkSqlString, this._sinkSqlFile) match {
          case (None, Some(_)) => this._sinkSqlString = this._sinkSqlFile.map(file => this.resolve(FileChannel.loadAsString(file))(config))
          case _ => this._sinkSqlString = this._sinkSqlString.map(stmt => this.resolve(stmt)(config))
        }
        validate(this._sinkSqlString, "The sink statement is required when the write is to merge.")
      case _ =>
    }
    validate(this._mode, "The write-mode for JdbcWriter is invalid or not defined.", Seq("overwrite", "append", "merge"))
  }

  /**
   * The write mode
   * @param mode
   * @return
   */
  def writeMode(mode: String): JdbcWriter = { this._mode = Some(mode); this }

  /**
   * The sink-sql-statement for merging data into target
   * @param stmt
   * @return
   */
  def sinkSqlString(stmt: String): JdbcWriter = { this._sinkSqlString = Some(stmt); this }
  /**
   * The sink-sql-file for merging data into target
   * @param file
   * @return
   */
  def sinkSqlFile(file: String): JdbcWriter = { this._sinkSqlFile = Some(file); this }

  /**
   * The source view
   * @param view
   * @return
   */
  def sourceView(view: String): JdbcWriter = { this._sourceView = Some(view); this }
}
