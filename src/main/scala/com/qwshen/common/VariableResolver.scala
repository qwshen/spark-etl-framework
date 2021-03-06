package com.qwshen.common

import com.qwshen.etl.configuration.ConfigurationManager
import com.typesafe.config.Config
import org.apache.spark.sql.SparkSession
import scala.util.{Success, Try}
import scala.util.matching.Regex

/**
 * To resolve varaibles with configuration
 */
trait VariableResolver {
  //the name of variable used for calculating the value of a sql-variable
  private final val _sys_var = "qwshen.s__y_s.v___ar__nam_e____"

  /**
   * calculate the value of the variable in case the variable calls one sql-function.
   * @param value - the value of a variable
   * @param session - the spark-session for calling sql-function
   * @return - the result of the call
   */
  def evaluate(value: String)(implicit session: SparkSession): String = {
    def callFun = (v: String) => VariableResolver.collectFunctions(session).exists(fun => v.contains(fun))
    Try {
      if (!callFun(value.toLowerCase)) {
        value
      } else {
        session.sql(String.format("set %s = %s", this._sys_var, ConfigurationManager.unquote(value)))
        import session.implicits._
        ConfigurationManager.quote(session.sql(String.format("select ${%s}", this._sys_var)).as[String].first())
      }
    }.toOption.getOrElse(value)
  }

  /**
   * Resolve a variable with configuration
   * @param input - the input string with variables to be resolved
   * @param config - the configuration container which has values of variables
   * @return - output string with variables resolved.
   */
  def resolve(input: String, exclusion: Seq[String] = Nil)(implicit config: Config): String = {
    val variables = exclusion.map(e => String.format("${%s}", e))
    def isExcluded: String => Boolean = s => variables.foldLeft(false)((r, v) => r | s.equalsIgnoreCase(v))
    VariableResolver.varPattern.findAllIn(input).toSeq.filter(s => !isExcluded(s)).foldLeft(input)((r, v) => {
      Try(config.getAnyRef(VariableResolver.getName(v))) match {
        case Success(cv) => r.replace(v, cv.toString)
        case _ => throw new RuntimeException(s"$v is not defined. Please check the configuration & pipeline definition.")
      }
    })
  }
}

object VariableResolver {
  //the spark udf-functions
  private var _udFuns: Seq[String] = Nil

  //the pattern of variables. Variables are defined as ${abc_xyz}
  lazy val varPattern: Regex = "\\$\\{([a-z|A-Z])+([a-z|A-Z|0-9|_|\\.|-])*\\}".r
  //pattern for extracting the name of the variable
  lazy val namePattern: Regex = "\\$\\{([^\\}]+)\\}".r.unanchored

  /**
   * Get the name of a variable: ${name} => name
   * @param name - the initial name of the variable
   * @return
   */
  def getName(name: String): String = {
    name match {
      case VariableResolver.namePattern(n) => n
      case _ => name
    }
  }

  /**
   * Collect all spark functions
   * @param session - the spark-session
   * @return - a collection of spark functions
   */
  def collectFunctions(session: SparkSession): Seq[String] = {
    if (this._udFuns.isEmpty) {
      import session.implicits._
      //exclude all functions that contain operators such as +, -, *, /, etc.
      val fnp = "[\\W]+".r
      this._udFuns = session.sql("show functions").as[String].filter(fun => fnp.findFirstIn(fun).isEmpty).collect().map(s => s.toLowerCase)
    }
    return this._udFuns;
  }
}