package com.it21learning.etl.setting

import com.it21learning.etl.common.{Actor, ExecutionContext}
import com.typesafe.config.Config
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.util.{Failure, Success, Try}
import scala.xml.NodeSeq

/**
 * Set up SQL variables
 */
class SqlVariableSetter(private val variables: Seq[(String, String)]) extends Actor {
  def this() = this(Nil)

  //container
  private var _variables: scala.collection.mutable.Map[String, String] = scala.collection.mutable.Map[String, String]()
  //append
  this.variables.foreach(v => this._variables.put(v._1, v._2))
  /**
   * All variables
   *
   * @return
   */
  def allVariables: Seq[(String, String)] = this._variables.toSeq

  /**
   * The sql-statement for setting up the sql variables
   * @return
   */
  def sqlStmt: String = this._variables.map { case(k, v) => s"set $k = $v" }.mkString(scala.util.Properties.lineSeparator)

  /**
   * Initialize the variables by the xml-definition
   *
   * @param definition - is defined as:
   *   <actor type="...">
   *     <variable name="name1" value="value1" />
   *     <variable name="name2" value="value2" /">
   *   </actor>
   *
   * @param config - the configuration object
   * @param session - the spark-session object
   *
   */
  override def init(definition: NodeSeq, config: Config)(implicit session: SparkSession): Unit = {
    super.init(config)
    (definition \ "variable").foreach(v => this._variables.put((v \ "@name").text, (v \ "@value").text))
  }

  /**
   * Set up the sql variables
   *
   * @param ctx - the execution context
   * @param session - the spark-session
   *  @return
   */
  def run(ctx: ExecutionContext)(implicit session: SparkSession): Option[DataFrame] = Try {
    //log the sql statement in debug mode
    if (this.logger.isDebugEnabled) {
      this.logger.info(s"Starting to execute sql statement - ${this.sqlStmt}.")
    }

    //if the sql-statement is empty, no need to run.
    if (this.sqlStmt != null && this.sqlStmt.nonEmpty) {
      //run the sql-statement
      session.sql(this.sqlStmt)
    }
  } match {
    case Success(_) => None
    case _ => throw new RuntimeException(s"Cannot run sql statements ${this.sqlStmt}.")
  }

  /**
   * Add a variable
   *
   * @param name
   * @param value
   * @return
   */
  def add(name: String, value: String): SqlVariableSetter = { this._variables.put(name, value); this }
}
