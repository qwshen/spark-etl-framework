package com.qwshen.etl.common

import com.qwshen.common.{PropertyKey, VariableResolver}
import com.qwshen.common.io.FileChannel
import com.typesafe.config.Config
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.catalyst.analysis.UnresolvedRelation
import org.apache.spark.sql.catalyst.plans.logical.SubqueryAlias
import scala.util.{Failure, Success, Try}

/**
 * The general SqlActor
 */
class SqlActor extends SqlBase[SqlActor]

/**
 * The base class for SQL actors
 * @tparam T
 */
private[etl] class SqlBase[T] extends Actor with VariableResolver { self: T =>
  @PropertyKey("sqlString", false)
  protected var _sqlStmt: Option[String] = None
  @PropertyKey("sqlFile", false)
  protected var _sqlFile: Option[String] = None

  //tables
  protected var _views: Seq[String] = Nil
  /**
   * Extra Views except the input views specified in the pipeline definition that are referenced/used by current actor
   * @return
   */
  override def extraViews: Seq[String] = this._views

  /**
   * Run the sql-statement
   *
   * @param ctx - the execution context
   * @param session - the spark-session
   *  @return
   */
  def run(ctx: ExecutionContext)(implicit session: SparkSession): Option[DataFrame] = for {
    stmt <- this._sqlStmt
  } yield {
    //log the sql statement in debug mode
    if (this.logger.isDebugEnabled) {
      this.logger.info(s"Starting to execute sql statement - $stmt.")
    }

    //run the sql-statement
    Try(session.sql(stmt)) match {
      case Success(df) => df
      case Failure(ex) => throw new RuntimeException(s"Running the sql-statement failed - $stmt.", ex)
    }
  }

  /**
   * Collect metrics of current actor
   * @param df
   * @param session
   * @return
   */
  override def collectMetrics(df: DataFrame)(implicit session: SparkSession): Seq[(String, String)] = this._sqlStmt.map(stmt => Seq(("sql-stmt", stmt))).getOrElse(Nil)

  /**
   * Initialize the actor with the properties & config
   */
  override def init(properties: Seq[(String, String)], config: Config)(implicit session: SparkSession): Unit = {
    super.init(properties, config)

    (this._sqlStmt, this._sqlFile) match {
      case (Some(_), _) =>
      case (_, Some(sf)) => this._sqlStmt = Some(FileChannel.loadAsString(sf))
      case _ => throw new RuntimeException("The sql-string & sql-file cannot be both empty in a sql actor.")
    }
    this._sqlStmt = this._sqlStmt.map(stmt => resolve(stmt)(config))
    //extract all tables in the sql-statement
    val alias = scala.collection.mutable.Set[String]()
    val relations = scala.collection.mutable.Set[String]()
    for (s <- this._sqlStmt) {
      val lp = session.sessionState.sqlParser.parsePlan(s)
      var i = 0
      while (lp(i) != null) {
        lp(i) match {
          case sa: SubqueryAlias => alias += sa.alias
          case r: UnresolvedRelation => relations += r.tableName
          case _ =>
        }
        i = i + 1
      }
    }
    this._views = relations.diff(alias).toSeq
  }

  /**
   * The sql-statement
   *
   * @param stmt
   * @return
   */
  def sqlString(stmt: String): T = { this._sqlStmt = Some(stmt); this }

  /**
   * The sql file
   *
   * @param file
   * @return
   */
  def sqlFile(file: String): T = { this._sqlStmt = Some(FileChannel.loadAsString(file)); this }
}
