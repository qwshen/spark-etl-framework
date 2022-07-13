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
  private final val _sql_variables = "qwshen.s__q_l.v___ar__i_ables____"
  /**
   * The statement trait
   */
  trait Statement {
    def text: String
  }

  /**
   * The regular sql-statement
   * @param text
   */
  case class SqlStatement(text: String) extends Statement { }

  /**
   * The regular set statement
   * @param variable - the name of the variable with value set
   * @param text - the value expression of the variable
   */
  case class SetStatement(variable: String, text: String) extends Statement { }

  /**
   * The set statement with select statement
   * @param variable - the name of the variable
   * @param text - the select statement from which the value of the variable is calcualted.
   */
  case class SetWithSelect(variable: String, text: String) extends Statement { }

  @PropertyKey("sqlString", false)
  protected var _sqlStmt: Option[String] = None
  @PropertyKey("sqlFile", false)
  protected var _sqlFile: Option[String] = None

  //statements
  protected val _stmts = new scala.collection.mutable.ArrayBuffer[Statement]()

  /**
   * Run the sql-statement
   *
   * @param ctx - the execution context
   * @param session - the spark-session
   *  @return - the dataframe from the last statement
   */
  override def run(ctx: JobContext)(implicit session: SparkSession): Option[DataFrame] = {
    //log the sql statement in debug mode
    if (this.logger.isDebugEnabled) {
      this.logger.info(s"Starting to execute sql statement - ${this._sqlStmt}.")
    }
    this._stmts match {
      case Seq(_, _ @ _*) => this._stmts.tail.foldLeft(this.execute(this._stmts.head, ctx))((_, stmt) => this.execute(stmt, ctx))
      case _ => None
    }
  }

  //execute one statement
  private def execute(stmt: Statement, ctx: JobContext)(implicit session: SparkSession): Option[DataFrame] = Try {
    this.flagReference(stmt, ctx)
    stmt match {
      case SetWithSelect(vn, text) =>
        val df = session.sql(text)
        val vv = df.first().get(0) match {
          case n: java.lang.Number => n.toString
          case a: Any => String.format("'%s'", a.toString)
        }
        session.sql(String.format("set %s = %s", vn, vv))
      case _ => session.sql(stmt.text)
    }
  } match {
    case Success(df) => Some(df)
    case Failure(ex) => throw new RuntimeException(s"Running the sql-statement failed - $stmt.", ex)
  }

  //collect view-references
  private def flagReference(stmt: Statement, ctx: JobContext)(implicit session: SparkSession): Unit = {
    //extract all tables in the sql-statement
    val alias = scala.collection.mutable.Set[String]()
    val relations = scala.collection.mutable.Set[String]()
    val lp = session.sessionState.sqlParser.parsePlan(stmt.text)
    var i = 0
    while (lp(i) != null) {
      lp(i) match {
        case sa: SubqueryAlias => alias += sa.alias
        case r: UnresolvedRelation => relations += r.tableName
        case _ =>
      }
      i = i + 1
    }
    relations.diff(alias).toSeq.foreach(view => ctx.viewReferenced(view))
  }

  /**
   * Collect metrics of current actor
   * @param df - the data-frame against it to collect metrics
   * @return - the metrics
   */
  override def collectMetrics(df: DataFrame): Seq[(String, String)] = this._sqlStmt.map(stmt => Seq(("sql-stmt", stmt))).getOrElse(Nil)

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

    var sqlVars = this.getSqlVariables.map(v => (v, v)).toMap
    for (stmt <- this._sqlStmt) {
      stmt.split(";").map(s => s.stripPrefix("[\r|\n]").stripSuffix("[\r|\n]").trim).foreach(s => {
        if (s.trim.replaceAll("[\r|\n]", " ").toLowerCase.startsWith("set ")) {
          val idx = s.substring(4).indexOf("=")
          if (idx < 0) {
            throw new RuntimeException("The set statement is invalid - $s");
          }
          val varName = s.substring(4, idx + 4).trim
          val varValue = this.resolve(s.substring(idx + 5).trim, sqlVars.keys.toSeq)(config)
          this._stmts.append(SetStatement(varName, String.format("set %s = %s", varName, varValue)))
          sqlVars += varName -> varValue
        } else if (s.trim.replaceAll("[\r|\n]", " ").toLowerCase.startsWith("setrun ")) {
          val idx = s.substring(7).indexOf("=")
          if (idx < 0) {
            throw new RuntimeException("The setrun statement is invalid - $s");
          }
          val varName = s.substring(7, idx + 7).trim
          val varValue = this.resolve(s.substring(idx + 8).trim, sqlVars.keys.toSeq)(config)
          this._stmts.append(if (isQuery(varValue)) SetWithSelect(varName, varValue) else SetStatement(varName, String.format("set %s = %s", varName, varValue)))
          sqlVars += varName -> varValue
        } else {
          this._stmts.append(SqlStatement(this.resolve(s, sqlVars.keys.toSeq)(config)))
        }
      })
    }
    this.setSqlVariables(sqlVars.keys.toSeq)
  }

  /**
   * Retrieve sql-variables defined by set statements
   * @param session - the spark-session object
   * @return - all sql variables defined in current session
   */
  protected def getSqlVariables(implicit session: SparkSession): Seq[String] = Try {
    import session.implicits._
    session.sql(String.format("select ${%s}", this._sql_variables)).as[String].first
  }match {
    case Success(s) => s.split(";")
    case _ => Nil
  }

  /**
   * Set combined sql-variables into a system-variable
   * @param variables - all sql-variables
   * @param session - the spark-session object
   */
  protected def setSqlVariables(variables: Seq[String])(implicit session: SparkSession): Unit = {
    session.sql(String.format("set %s = `'%s'`", this._sql_variables, variables.mkString(";")))
  }

  /**
   * Check if a statement is select or with ... select ... from
   * @param stmt - the input sql-statement
   * @return - true if it is a select
   */
  protected def isQuery(stmt: String): Boolean = {
    val s =  stmt.trim.replaceAll("[\r|\n]", " ").toLowerCase
    Seq("^\\(?select.+\\)?$", "^\\(?with.+select.+from.+\\)?$").map(e => e.r.findFirstIn(s).nonEmpty).reduce((x, y) => x | y)
  }

  /**
   * Check if a statement is a DML operation
   * @param stmt - the input sql-statement
   * @return - true if it is a DML
   */
  protected def isDML(stmt: String): Boolean = {
    val s =  stmt.trim.replaceAll("[\r|\n]", " ").toLowerCase
    Seq("^[insert|update|merge|delete].+", "^with.+[insert|update|merge|delete].+").map(e => e.r.findFirstIn(s).nonEmpty).reduce((x, y) => x | y)
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
