package com.qwshen.etl.test

import com.qwshen.etl.pipeline.PipelineRunner
import com.typesafe.config.{Config, ConfigFactory}
import org.apache.spark.sql.SparkSession
import org.scalatest.{FunSuite, Matchers}

import scala.util.{Failure, Properties, Success, Try}
import com.qwshen.common.io.FileChannel
import com.qwshen.etl.common.PipelineContext
import com.qwshen.etl.pipeline.builder.PipelineFactory

import scala.util.control.Exception.ultimately

class TestApp extends FunSuite with Matchers {
  protected val resourceRoot: String = "file://" + java.net.URLDecoder.decode(getClass.getClassLoader.getResource("").getPath, "UTF-8")

  protected val config: Config = loadConfig()
  protected val runner = new PipelineRunner(new PipelineContext())

  protected def start(): Option[SparkSession] = Try {
    val session = createSparkSession()

    //turn off info logs
    session.sparkContext.setLogLevel("WARN")
    //disable writing crc files
    org.apache.hadoop.fs.FileSystem.get(session.sparkContext.hadoopConfiguration).setWriteChecksum(false)
    //disable writing __SUCCESS file
    session.sparkContext.hadoopConfiguration.set("mapreduce.fileoutputcommitter.marksuccessfuljobs", "false")

    session
  } match {
    case Success(session) => Some(session)
    case Failure(t) => throw t
  }

  protected def run(pipelineFile: String, prepare: Option[SparkSession => Unit] = None): Unit = {
    for {
      session <- this.start()
      pipeline <- PipelineFactory.fromFile(pipelineFile)(config, session)
    } ultimately {
      this.done(session)
    } {
      prepare.foreach(p => p(session))
      this.runner.run(pipeline)(session)
    }
  }

  protected def done(session: SparkSession): Unit = {
    session.stop()
  }

  protected def createSparkSession(): SparkSession = SparkSession.builder()
    .appName("test")
    .master("local[*]")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    .getOrCreate()

  protected def loadConfig(): Config = {
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

  protected def loadContent(file: String): String = FileChannel.loadAsString(file)
}
