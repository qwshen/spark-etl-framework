package com.qwshen.etl.sink

import com.mongodb.spark.MongoSpark
import com.mongodb.spark.config.WriteConfig
import com.qwshen.common.{PropertyKey, PropertyUse}
import com.qwshen.etl.common.{JobContext, MongoActor}
import com.typesafe.config.Config
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.util.{Failure, Success, Try}

/**
 * Write to mongo-db
 */
class MongoWriter extends MongoActor[MongoWriter] {
  //mode
  @PropertyKey("mode", false)
  protected var _mode: Option[String] = Some("overwrite")
  //view
  @PropertyKey("view", true)
  protected var _view: Option[String] = None

  /**
   * Run the actor
   *
   * @param ctx     - the execution context
   * @param session - the spark-session
   * @return
   */
  override def run(ctx: JobContext)(implicit session: SparkSession): Option[DataFrame] = for {
    hostName <- this._host
    portNum <- this._port
    dbName <- this._database
    collectionName <- this._collection
    mode <- this._mode
    df <- this._view.flatMap(name => ctx.getView(name))
  } yield Try {
    val uri = (this._user, this._password) match {
      case (Some(usr), Some(pwd)) => s"mongodb://$usr:$pwd@$hostName:$portNum/$dbName.$collectionName"
      case _ => s"mongodb://$hostName:$portNum/$dbName.$collectionName"
    }
    val wc = WriteConfig(Map("uri" -> uri) ++ this._options, Some(WriteConfig(session)))
    MongoSpark.save(df.write.mode(mode), wc)
  } match {
    case Success(_) => df
    case Failure(ex) => throw new RuntimeException(s"Cannot write to source - ${this._database}.${this._collection}.", ex)
  }

  /**
   * Initialize the Mongo-Writer
   */
  override def init(properties: Seq[(String, String)], config: Config)(implicit session: SparkSession): Unit = {
    super.init(properties, config)

    validate(this._mode, "The mode in MongoWriter is mandatory.", Seq("overwrite", "append"))
  }

  /**
   * The write behavior must be either overwrite or append. Default: overwrite
   * @param md
   * @return
   */
  def mode(md: String): MongoWriter = { this._mode = Some(md); this }
}

