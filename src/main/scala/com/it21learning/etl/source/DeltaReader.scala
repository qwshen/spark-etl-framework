package com.it21learning.etl.source

import com.it21learning.etl.common.{DeltaReadActor, ExecutionContext}
import org.apache.spark.sql.{DataFrame, SparkSession}
import scala.util.{Failure, Success, Try}

/**
 * This reader reads data from delta lake into a data-frame.
 *
 * The following is the definition in xml format
 *   <actor type="com.it21learning.etl.source.DeltaReader">
 *     <!-- options for the reading -->
 *     <property name="options">
 *       <!-- read -->
 *       <option name="timestampAsOf" value="2018-09-18T11:15:21.021Z" />
 *       <option name="versionAsOf" value="11" />
 *     </property>
 *     <!-- The source location where the data-frame is reading from. Only the first definition is used. This is mandatory -->
 *     <property name="source">
 *       <definition name="path">/mnt/delta/events</definition>
 *       <definition name="table">events</definition>
 *     </property>
 *   </actor>
 */
final class DeltaReader extends DeltaReadActor[DeltaReader] {
  /**
   * Execute the action
   *
   * @param ctx     - the execution context
   * @param session - the spark-session
   *  @return
   */
  def run(ctx: ExecutionContext)(implicit session: SparkSession): Option[DataFrame] = Try {
    //the initial DataframeReader
    val reader = this._options.foldLeft(session.read.format("delta"))((r, o) => r.option(o._1, o._2))
    //load
    (this._sourceType, this._sourceLocation) match {
      case (Some(tpe), Some(path)) if (tpe == "path") => reader.load(path)
      case (Some(tpe), Some(table)) if (tpe == "table") => reader.table(table)
      case _ => throw new RuntimeException("The source type and/or source location is/are invalid.")
    }
  } match {
    case Success(df) => Some(df)
    case Failure(t) => throw new RuntimeException(s"Load from delta ${this._sourceLocation.getOrElse("")} failed.", t)
  }
}
