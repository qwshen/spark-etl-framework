package com.it21learning.etl

import com.it21learning.common.io.FileChannel
import com.it21learning.etl.configuration.ArgumentParser
import com.it21learning.etl.pipeline.PipelineRunner
import com.it21learning.etl.pipeline.builder.PipelineFactory
import com.typesafe.config.Config
import org.apache.spark.sql.SparkSession
import org.apache.hadoop.fs.FileSystem
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
 *     --class com.it21learning.etl.Launcher spark-etl-framework-0.1-SNAPSHOT.jar \
 *     --pipeline-def ./test.yaml --application-conf ./application.conf \
 *     --var process_date=20200921 --var environment=dev \
 *     --vars encryption_key=/tmp/app.key,password_key=/tmp/pwd.key \
 *     --staging-uri hdfs://tmp/staging --staging-actions load-events,combine-users-events \
 */
object Launcher {
  def main(args: Array[String]): Unit = {
    val arguments = ArgumentParser.parse(args)

    implicit val config: Config = arguments.config
    implicit val session: SparkSession = createSparkSession
    try {
      val pipelineString = FileChannel.loadAsString(arguments.pipelineFile)
      for {
        pipeline <-
          if (arguments.pipelineFile.endsWith(".yaml")) {
            PipelineFactory.fromYaml(pipelineString)
          } else if (arguments.pipelineFile.endsWith(".json")) {
            PipelineFactory.fromJson(pipelineString)
          } else if (arguments.pipelineFile.endsWith(".xml")) {
            PipelineFactory.fromXml(pipelineString)
          }
          else {
            throw new RuntimeException("The pipeline definition must be in the format of one of yaml, json & xml file.")
          }
      } {
        new PipelineRunner(new ApplicationContext()).run(pipeline)
      }
    }
    finally {
      session.stop()
    }
  }

  //create the spark-session
  private def createSparkSession(implicit config: Config): SparkSession = if (!config.hasPath("application.runtime")) {
    SparkSession.builder.getOrCreate()
  } else {
    var builder: SparkSession.Builder = config.getConfig("application.runtime").entrySet().asScala
      .filter(_.getKey.startsWith("spark"))
      .foldLeft(SparkSession.builder)((b, e) => {
        if (e.getKey.contains("master")) {
          b.master(e.getValue.unwrapped().toString)
        } else {
          b.config(e.getKey, e.getValue.unwrapped().toString)
        }
      })

    if (Try(config.getBoolean("application.runtime.hiveSupport")).getOrElse(false)) {
      builder = builder.enableHiveSupport()
    }

    val session = builder.getOrCreate()
    if (Try(config.getBoolean("application.runtime.filesystem.skip.write.checksum")).getOrElse(false)) {
      FileSystem.get(session.sparkContext.hadoopConfiguration).setWriteChecksum(false)
    }
    config.getConfig("application.runtime.hadoopConfiguration").entrySet().asScala
      .foldLeft(session)((s, e) => {
        s.sparkContext.hadoopConfiguration.set(e.getKey, e.getValue.unwrapped().toString)
        s
      })
  }
}
