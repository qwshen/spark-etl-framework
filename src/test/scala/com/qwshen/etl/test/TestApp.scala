package com.qwshen.etl.test

import com.typesafe.config.{Config, ConfigFactory}
import org.apache.spark.sql.SparkSession
import org.scalatest.{FunSuite, BeforeAndAfterEach, Matchers}
import scala.util.Properties
import scala.io.Source

trait TestApp extends FunSuite with BeforeAndAfterEach with Matchers {
  protected val resourceRoot: String = getClass.getClassLoader.getResource("").getPath

  def loadConfig(): Config = {
    val cfgString = loadContent(this.resourceRoot + "application-test.conf")
    val config = ConfigFactory.parseString(cfgString)

    val cfgOverride = Seq(
      String.format("events.users_input = \"%s\"", s"${resourceRoot}data/users"),
      String.format("events.events_input = \"%s\"", s"${resourceRoot}data/events"),
      String.format("events.train_input = \"%s\"", s"${resourceRoot}data/train"),
      String.format("application.scripts_uri = \"%s\"", s"${resourceRoot}scripts")
    ).mkString(Properties.lineSeparator)
    ConfigFactory.parseString(cfgOverride).withFallback(config)
  }

  def loadContent(file: String): String = {
    val source = Source.fromFile(file)
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

  //turn off info logs
  session.sparkContext.setLogLevel("WARN")
  //disable writing crc files
  org.apache.hadoop.fs.FileSystem.get(session.sparkContext.hadoopConfiguration).setWriteChecksum(false)
  //disable writing __SUCCESS file
  session.sparkContext.hadoopConfiguration.set("mapreduce.fileoutputcommitter.marksuccessfuljobs", "false")
}