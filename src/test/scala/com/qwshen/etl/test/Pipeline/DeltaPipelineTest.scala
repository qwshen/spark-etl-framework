package com.qwshen.etl.test.Pipeline

import com.qwshen.etl.test.TestApp
import org.apache.spark.sql.SparkSession

class DeltaPipelineTest extends TestApp {
  test("Pipeline test - file read / delta write") {
    this.run(s"${resourceRoot}pipelines/pipeline_fileRead-deltaWrite.yaml")
  }

  test("Pipeline test - delta read / file write") {
    this.run(s"${resourceRoot}pipelines/pipeline_deltaRead-fileWrite.json")
  }

  test("Pipeline test - delta streaming read / kafka streaming write") {
    this.run(s"${resourceRoot}pipelines/pipeline_deltaStreamRead-kafkaStreamWrite.xml")
  }

  test("Pipeline test - kafka streaming read / delta streaming write") {
    this.run(s"${resourceRoot}pipelines/pipeline_kafkaStreamRead-deltaStreamWrite.yaml")
  }

  override def createSparkSession(): SparkSession = SparkSession.builder()
    .appName("test")
    .master("local[*]")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
      .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .getOrCreate()
}
