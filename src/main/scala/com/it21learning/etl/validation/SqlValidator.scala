package com.it21learning.etl.validation

import com.it21learning.etl.common.{Actor, ExecutionContext}
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * Validate result by checking the view specified by viewName. The view must have the following fields:
 *   - validationCode
 *   - validationDescription
 *   - validationLevel:
 *     - error: error out;
 *     - warning: log warning but proceed.
 *
 *  Note: if there are more than 3 rows in the view, only first 3 rows are picked.
 *
 *  The following is one definition in xml format
 *
 *  <actor type="com.it21learning.etl.validation.SqlValidator">
 *    <!-- the view to be validated. This is mandatory -->
 *    <property name="viewName">events</property>
 *  </actor>
 */
final class SqlValidator() extends Actor {
  //the view
  private var viewName: Option[String] = None

  /**
   * Run the actor
   *
   * @param ctx - the execution context
   * @param session - the spark-session
   * @return
   */
  def run(ctx: ExecutionContext)(implicit session: SparkSession): Option[DataFrame] = for {
    _ <- validate(this.viewName, "The viewName in SqlValidator is mandatory.")
    df <- this.viewName.flatMap(name => ctx.getView(name))
  } yield {
    import session.implicits._
    //pick up the first 3 rows
    val rows: Seq[ValidationRow] = df.as[ValidationRow].head(3)
    if (rows.nonEmpty) {
      val isError: Boolean = rows.map(r => r.validationLevel.equalsIgnoreCase("error")).reduce(_ | _)
      //message
      val message = rows.map(r => Seq(s"Code: ${r.validatonCode}", s"Description: ${r.validationDescription}")).mkString(scala.util.Properties.lineSeparator)
      if (isError) {
        //error out
        throw new RuntimeException(s"Validation failed due to the following error: ${message}")
      }
      else {
        //log warning
        this.logger.warn(s"Validation generates the following warning: ${message}")
      }
    }
    df
  }

  /**
   * The view to be validated.
   *
   * @param view
   * @return
   */
  def forView(view: String): SqlValidator = { this.viewName = Some(view); this }
}
