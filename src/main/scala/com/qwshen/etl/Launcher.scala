package com.qwshen.etl

import com.qwshen.etl.common.PipelineContext
import com.qwshen.etl.configuration.ArgumentParser
import com.qwshen.etl.pipeline.PipelineRunner
import com.qwshen.etl.pipeline.builder.PipelineFactory
import com.typesafe.config.Config
import org.apache.spark.sql.SparkSession
import org.apache.hadoop.fs.FileSystem
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.JavaConverters._
import scala.util.Try

/**
 * The etl launcher to start running a etl-pipeline
 *
 * The job submit command line:
 *
 *   spark-submit --master yarn|local --deploy-mode client|cluster \
 *     --name test \
 *     --conf spark.executor.memory=24g --conf spark.driver.memory=16g \
 *     --conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog \
 *     --jars ./mysql-connector-jar.jar,./mongo-jara-driver-3.9.1.jar \
 *     --class com.qwshen.etl.Launcher spark-etl-framework-0.1-SNAPSHOT.jar \
 *     --pipeline-def "./test.yaml#load users;transform-user-events" --application-conf ./application.conf \
 *     --var process_date=20200921 --var environment=dev \
 *     --vars encryption_key=/tmp/app.key,password_key=/tmp/pwd.key \
 *     --staging-uri hdfs://tmp/staging --staging-actions load-events,combine-users-events \
 *     --metrics-logging-uri hdfs://tmp/metrics-logging --metrics-logging-actions load-events,combine-users-events
 */
class Launcher {
  /*
   * Run a pipeline-job
   */
  def run(args: Array[String]): Unit = {
    val arguments = ArgumentParser.parse(args)

    implicit val config: Config = arguments.config
    val session: SparkSession = createSparkSession
    try {
      val (pipelineFile, jobName) = arguments.pipelineFile.split("#").toSeq match {
        case Seq(head, tail @ _*) => (head, tail.headOption)
        case _ => (arguments.pipelineFile, None)
      }
      for {
        pipeline <- PipelineFactory.fromFile(pipelineFile)(config, session.newSession())
      } {
        //customize staging behavior
        arguments.stagingBehavior.foreach(behavior => pipeline.takeStagingBehavior(behavior))
        //customize metrics logging
        arguments.metricsLoggingBehavior.foreach(behavior => pipeline.takeMetricsLogging(behavior))

        new PipelineRunner(new PipelineContext()).run(pipeline, jobName.map(x => x.split("[;|,]").toSeq).getOrElse(Nil))(session)
      }
    }
    finally {
      recycleSparkSession(session)
    }
  }

  /*
   * Create the spark-session
   */
  def createSparkSession(implicit config: Config): SparkSession = if (!config.hasPath("application.runtime")) {
    SparkSession.builder.getOrCreate()
  } else {
    val sparkConf = config.getConfig("application.runtime").entrySet().asScala
      .filter(_.getKey.startsWith("spark"))
      .foldLeft(new SparkConf())((c, e) => c match {
        case s if s.contains("app.name") => c.setAppName(e.getValue.unwrapped().toString)
        case s if s.contains("master") => c.setMaster(e.getValue.unwrapped().toString)
        case _ => c.set(e.getKey.replace("\"", ""), e.getValue.unwrapped().toString)
      })

    val sparkCtx = SparkContext.getOrCreate(sparkConf)
    config.getConfig("application.runtime.hadoopConfiguration").entrySet().asScala
      .foldLeft(sparkCtx)((ctx, e) => {
        ctx.hadoopConfiguration.set(e.getKey, e.getValue.unwrapped().toString)
        ctx
      })

    var builder = SparkSession.builder
    if (Try(config.getBoolean("application.runtime.hiveSupport")).getOrElse(false)) {
      builder = builder.enableHiveSupport()
    }
    val session = builder.getOrCreate()
    if (Try(config.getBoolean("application.runtime.filesystem.skip.write.checksum")).getOrElse(false)) {
      FileSystem.get(session.sparkContext.hadoopConfiguration).setWriteChecksum(false)
    }
    session
  }

  private def recycleSparkSession(implicit session: SparkSession): Unit = {
    if (!sys.env.contains("DATABRICKS_RUNTIME_VERSION")) {
      session.stop();
    }
  }
}

object Launcher {
  def main(args: Array[String]): Unit = new Launcher().run(args)
}
