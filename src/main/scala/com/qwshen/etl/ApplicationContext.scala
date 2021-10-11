package com.qwshen.etl

import java.nio.file.Paths
import java.text.SimpleDateFormat
import java.util.Calendar
import com.qwshen.common.logging.Loggable

/**
 * The application-context describes the common and environment variables at the application level
 */
final class ApplicationContext() extends Loggable {
  /**
   * The name of the global database in spark-sql
   */
  val global_db: String = "global_temp"

  /**
   * The date-time format as of yyyy-MM-dd HH:mm:ss
   */
  lazy val fmtDateTime: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
  /**
   * The date-format as of yyyy-MM-dd
   */
  lazy val fmtDate: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd")
  /**
   * The time-format as of HH:mm:ss
   */
  lazy val fmtTime: SimpleDateFormat = new SimpleDateFormat("HH:mm:ss")
  /**
   * The year-format as of Y
   */
  lazy val fmtYear: SimpleDateFormat = new SimpleDateFormat("Y")
  /**
   * The month-format as of M
   */
  lazy val fmtMonth: SimpleDateFormat = new SimpleDateFormat("M")
  /**
   * The day-format as of d
   */
  lazy val fmtDay: SimpleDateFormat = new SimpleDateFormat("d")
  /**
   * The day-of-week format as of u
   */
  lazy val fmtDayOfWeek: SimpleDateFormat = new SimpleDateFormat("u")

  /**
   * The current date-time in the format of yyyy-MM-dd HH:mm:ss
   *
   * @return
   */
  def currentDateTime: String = fmtDateTime.format(Calendar.getInstance().getTime)

  /**
   * The current date in the format of yyyy-MM-dd
   *
   * @return
   */
  def currentDate: String = fmtDate.format(Calendar.getInstance().getTime)

  /**
   * The current time in the format of HH:mm:ss
   *
   * @return
   */
  def currentTime: String = fmtTime.format(Calendar.getInstance().getTime)

  /**
   * The current year in the format of yyyy
   *
   * @return
   */
  def currentYear: String = fmtYear.format(Calendar.getInstance().getTime)

  /**
   * The current month in the format of MM
   *
   * @return
   */
  def currentMonth: String = fmtMonth.format(Calendar.getInstance().getTime)

  /**
   * The current day in the format of dd
   *
   * @return
   */
  def currentDay: String = fmtDay.format(Calendar.getInstance().getTime)

  /**
   * The current day of week
   * 1 --> Monday, ..., 7 --> Sunday
   *
   * @return
   */
  def currentDayOfWeek: String = fmtDayOfWeek.format(Calendar.getInstance().getTime)

  /**
   * The current work-directory where the application started.
   *
   * @return
   */
  def workDirectory: String = Paths.get("").toAbsolutePath.toString

  /**
   * Defines the number of io (jdbc, hbase, ...) connections to target database.
   * @return
   */
  def ioConnections: Int = 16

  /**
   * Defines the number of rows in one batch when writing to target sink
   * @return
   */
  def ioBatchSize: Int = 1600
}
