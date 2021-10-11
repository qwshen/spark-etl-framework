package com.qwshen.etl.test.stream

import com.qwshen.common.PropertyKey
import com.qwshen.etl.common.stream.ArbitraryStatefulProcessor
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.{GroupState, GroupStateTimeout}
import scala.util.Try

class UserStatefulProcessor extends ArbitraryStatefulProcessor with Serializable {
  /**
   * The timeput-type which, if specified, should be one of ProcessingTimeTimeout, EventTimeTimeout and NoTimeout.
   */
  @PropertyKey("timeoutType", false)
  private var _timeoutType: Option[String] = None

  /**
   * The timeout-duration which, if specified, should be in the format of "3 hours", "51 minutes", etc.
   */
  @PropertyKey("timeoutDuration", false)
  private var _timeoutDuration: Option[String] = None

  /**
   * The method to implement the custom logic of state transformation.
   *
   * @param df
   * @return
   */
  def transformState(df: DataFrame): DataFrame = {
    import df.sparkSession.implicits._

    df.filter('age.isNotNull)
      .withColumn("gender", coalesce('gender, lit("unknown")))
      .withColumn("interested", coalesce('interested, lit(0)))
    .as[InputUser]
      .groupByKey(u => (u.gender.get, u.interested.get)).mapGroupsWithState(
        this._timeoutType match {
          case Some(t) if (t == "ProcessingTimeout") => GroupStateTimeout.ProcessingTimeTimeout
          case Some(t) if (t == "EventTimeTimeout") => GroupStateTimeout.EventTimeTimeout
          case _ => GroupStateTimeout.NoTimeout
        }
      )(mappingFunction).toDF
  }

  def mappingFunction(key: (String, Int), users: Iterator[InputUser], state: GroupState[AgeState]): OutputAge = {
    val curState = state.getOption.getOrElse(new AgeState(Some(key._1), Some(key._2), None, None, None, None, None, None))
    val usrs = users.toList
    val newState = AgeState(
      gender = Some(key._1),
      interested = Some(key._2),
      minAge = curState.minAge match {
        case Some(a) => Some(math.min(a, Try(usrs.filter(_.age.isDefined).map(u => u.age.get).min).getOrElse(999)))
        case _ => Try(usrs.filter(_.age.isDefined).map(u => u.age.get).min).toOption
      },
      maxAge = curState.maxAge match {
        case Some(a) => Some(math.max(a, Try(usrs.filter(_.age.isDefined).map(u => u.age.get).max).getOrElse(-999)))
        case _ => Try(usrs.filter(_.age.isDefined).map(u => u.age.get).max).toOption
      },
      sum = Some(usrs.map(u => u.age.get.toLong).sum + curState.sum.getOrElse(0L)),
      count = Some(usrs.length + curState.count.getOrElse(0)),
      start = curState.start match {
        case Some(s) => Try(usrs.map(u => u.timeStamp).foldLeft(s)((r, t) => if (r.after(t)) t else r)).toOption
        case _ => Try(usrs.map(u => u.timeStamp).reduce((x, y) => if(x.after(y)) y else x)).toOption
      },
      end = curState.end match {
        case Some(s) => Try(usrs.map(u => u.timeStamp).foldLeft(s)((r, t) => if (r.before(t)) t else r)).toOption
        case _ => Try(usrs.map(u => u.timeStamp).reduce((x, y) => if(x.before(y)) y else x)).toOption
      }
    )
    state.update(newState)

    val avgAge = (newState.sum, newState.count) match {
      case (Some(s), Some(c)) if (c > 0) => Some(s.toFloat / c.toFloat)
      case _ => None
    }
    OutputAge(key._1, key._2, newState.minAge, newState.maxAge, avgAge, newState.start, newState.end)
  }
}
