package com.qwshen.etl.source

import com.qwshen.etl.common.{ExecutionContext, MongoActor}
import org.apache.spark.sql.{DataFrame, SparkSession}
import com.mongodb.spark.config.ReadConfig
import com.mongodb.spark.MongoSpark
import scala.util.{Failure, Success, Try}

/**
 * Read from mongo-db
 */
final class MongoReader extends MongoActor[MongoReader] {
  /**
   * Run the jdbc-reader
   *
   * @param ctx - the execution context
   * @param session - the spark-session
   * @return
   */
  def run(ctx: ExecutionContext)(implicit session: SparkSession): Option[DataFrame] = for {
    hostName <- this._host
    portNum <- this._port
    dbName <- this._database
    collectionName <- this._collection
  } yield Try {
    val uri = (this._user, this._password) match {
      case (Some(usr), Some(pwd)) => s"mongodb://$usr:$pwd@$hostName:$portNum/$dbName.$collectionName"
      case _ => s"mongodb://$hostName:$portNum/$dbName.$collectionName"
    }
    val rc = ReadConfig(Map("uri" -> uri) ++ this._options, Some(ReadConfig(session)))
    MongoSpark.load(session, rc)
  } match {
    case Success(df) => df
    case Failure(ex) => throw new RuntimeException(s"Cannot load from source - ${this._database}.${this._collection}.", ex)
  }
}
