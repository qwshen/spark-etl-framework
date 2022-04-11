package com.qwshen.etl.sink

import com.qwshen.etl.common.{ExecutionContext, IcebergActor}
import org.apache.spark.sql.{DataFrame, SparkSession}
import com.qwshen.common.PropertyKey
import com.typesafe.config.Config
import scala.util.{Failure, Success, Try}

/**
 * To write a dataframe into a iceberg table
 */
class IcebergWriter extends IcebergActor[IcebergWriter] {
  //the mode - must be one of overwrite, append
  @PropertyKey("mode", true)
  protected var _mode: Option[String] = None

  //the view to be written
  @PropertyKey("view", true)
  protected var _view: Option[String] = None

  /**
   * Initialize the actor with the properties & config
   */
  override def init(properties: Seq[(String, String)], config: Config)(implicit session: SparkSession): Unit = {
    super.init(properties, config)

    //the mode must be one of overwrite, append
    this.validate(this._mode, "The mode in IcebergWriter must be either overwrite or append.", Seq("overwrite", "append"))
  }

  /**
   * Run the actor
   * @param ctx     - the execution context
   * @param session - the spark-session
   * @return
   */
  def run(ctx: ExecutionContext)(implicit session: SparkSession): Option[DataFrame] = for {
    location <- this._location
    mode <- this._mode
    df <- this._view.flatMap(name => ctx.getView(name))
  } yield Try {
      this._options.foldLeft(df.write.format("iceberg"))((w, o) => w.option(o._1, o._2)).mode(mode).save(location)
  } match {
    case Success(_) => df
    case Failure(ex) => throw new RuntimeException(s"Cannot write data to the target - $location.", ex)
  }

  /**
   * The write mode. The valid values are: append, overwrite.
   *
   * @param value
   * @return
   */
  def mode(value: String): IcebergWriter = { this._mode = Some(value); this }

  /**
   * The name of the view to be written
   *
   * @param value
   * @return
   */
  def view(value: String): IcebergWriter = { this._view = Some(value); this }
}
