package com.it21learning.etl.test

import com.typesafe.config.{Config, ConfigFactory}
import org.apache.spark.sql.SparkSession
import org.scalatest.{FunSuite, BeforeAndAfterEach, Matchers}
import scala.util.Properties
import java.net.URL
import scala.io.Source

trait TestApp extends FunSuite with BeforeAndAfterEach with Matchers {
  protected val clsLoader: ClassLoader = getClass.getClassLoader

  def loadConfig(): Config = {
    val cfgString = loadContent(clsLoader.getResource("application-test.conf"))
    val config = ConfigFactory.parseString(cfgString)

    val cfgOverride = Seq(
      String.format("events.users_input = \"%s\"", clsLoader.getResource("data/users")),
      String.format("events.events_input = \"%s\"", clsLoader.getResource("data/events")),
      String.format("events.train_input = \"%s\"", clsLoader.getResource("data/train")),
      String.format("application.scripts_uri = \"%s\"", clsLoader.getResource("scripts"))
    ).mkString(Properties.lineSeparator)
    ConfigFactory.parseString(cfgOverride).withFallback(config)
  }

  def loadContent(url: URL): String = {
    val source = Source.fromURL(url)
    try {
      source.getLines().mkString(Properties.lineSeparator)
    } finally {
      source.close()
    }
  }
}

trait SparkApp extends TestApp {
  implicit val session: SparkSession = SparkSession.builder().appName("test")
    .master("local[*]")
    .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .getOrCreate()

  //disable writing crc files
  org.apache.hadoop.fs.FileSystem.get(session.sparkContext.hadoopConfiguration).setWriteChecksum(false)
  //disable writing __SUCCESS file
  session.sparkContext.hadoopConfiguration.set("mapreduce.fileoutputcommitter.marksuccessfuljobs", "false")
}