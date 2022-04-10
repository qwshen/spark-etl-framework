package com.qwshen.etl.test.Pipeline

import com.qwshen.etl.pipeline.builder.PipelineFactory
import scala.util.control.Exception.ultimately
import com.qwshen.etl.test.TestApp
import org.apache.spark.sql.SparkSession

class IcebergPipelineTest extends TestApp {
  test("Pipeline test - file read / iceberg write") {
    for {
      session <- this.start()
      pipeline <- PipelineFactory.fromXml(loadContent(s"${resourceRoot}pipelines/pipeline_fileRead-icebergWrite.xml"))(config, session)
    } ultimately {
      this.done(session)
    } {
      runner.run(pipeline)(session)
    }
  }

//  test(   "Pipeline test - delta read / file write") {
//    for (pipeline <- PipelineFactory.fromJson(loadContent(s"${resourceRoot}pipelines/pipeline_deltaRead-fileWrite.json"))) {
//      runner.run(pipeline)
//    }
//  }
//
//  test("Pipeline test - delta streaming read / kafka streaming write") {
//    for (pipeline <- PipelineFactory.fromXml(loadContent(s"${resourceRoot}pipelines/pipeline_deltaStreamRead-kafkaStreamWrite.xml"))) {
//      runner.run(pipeline)
//    }
//  }
//
//  test("Pipeline test - kafka streaming read / delta streaming write") {
//    for (pipeline <- PipelineFactory.fromYaml(loadContent(s"${resourceRoot}pipelines/pipeline_kafkaStreamRead-deltaStreamWrite.yaml"))) {
//      runner.run(pipeline)
//    }
//  }

  override def createSparkSession(): SparkSession = SparkSession.builder()
    .appName("test")
    .master("local[*]")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")

      //SparkSessionCatalog handles both iceberg and non-iceberg tables
      .config("spark.sql.catalog.spark_catalog", "org.apache.iceberg.spark.SparkSessionCatalog")
      .config("spark.sql.catalog.spark_catalog.type", "hive")
      //.config("spark.sql.catalog.spark_catalog.uri", "file:///tmp/spark-warehouse")
      //.config("spark.sql.catalog.spark_catalog.type", "hadoop")
      //.config("spark.sql.catalog.spark_catalog.warehouse", "/tmp/spark-warehouse")

      //SparkCatalog handles only iceberg tables
      .config("spark.sql.catalog.events", "org.apache.iceberg.spark.SparkCatalog")
      .config("spark.sql.catalog.events.type", "hadoop")
      .config("spark.sql.catalog.events.warehouse", "/tmp/events-warehouse")
    .getOrCreate()
}

