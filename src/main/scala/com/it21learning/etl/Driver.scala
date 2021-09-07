package com.it21learning.etl

import com.it21learning.etl.configuration.ArgumentParser
import com.it21learning.etl.pipeline.{PipelineRunner, builder}
import com.it21learning.etl.pipeline.builder.XmlPipelineBuilder
import com.it21learning.etl.pipeline.definition.{Pipeline, PipelineBuilder}
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
 *     --conf spark.executor.memory=24g --conf spark.driver.memory=16g \
 *     --conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog \
 *     --jars ./mysql-connector-jar.jar,./mongo-jara-driver-3.9.1.jar \
 *     --class com.it21learning.etl.EtlLauncher spark-etl-framework-0.1-SNAPSHOT.jar \
 *     --topology-class com.it21learning.etl.builder.XmlTopology --definition ./test.xml \
 *     --var process_date=20200921 --var environment=dev \
 *     --vars encryption_key=/tmp/app.key,password_key=/tmp/pwd.key \
 *     --staging-uri hdfs://tmp/staging --staging-actions load-events,combine-users-events \
 *     --app-conf ./application.conf --app-name test
 */
object Driver {
  def main(args: Array[String]): Unit = {
    //parse the arguments
    val arguments = ArgumentParser.parse(args)

    implicit val config: Config = arguments.config
    implicit val session: SparkSession = createSparkSession
    try {
//      //topology
//      val pipeline: Option[Pipeline] = if (arguments.topologyClass == classOf[builder.XmlPipelineBuilder].getName) {
//        //requires the xml definition file
//        if (arguments.definitionFile.isEmpty) {
//          throw new RuntimeException("The xml file for the pipeline definition is not provided.")
//        }
//        new XmlPipelineBuilder().build(arguments.definitionFile.get)
//      }
//      else {
//        //use custom Topology to build pipeline
//        Class.forName(arguments.topologyClass).newInstance().asInstanceOf[PipelineBuilder].build(arguments.definitionFile.get)
//      }
//
//      //run the pipeline
//      pipeline.foreach(pl => new PipelineRunner(new ApplicationContext(config)).run(pl))
    }
    finally {
      //stop the session
      session.stop()
    }
  }

  //create the spark-session
  private def createSparkSession(implicit config: Config): SparkSession = {
    var builder: SparkSession.Builder = config.getConfig("application.runtime").entrySet().asScala
      .filter(_.getKey.startsWith("spark"))
      .foldLeft(SparkSession.builder.appName("etl-pipeline"))((b, e) => {
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
