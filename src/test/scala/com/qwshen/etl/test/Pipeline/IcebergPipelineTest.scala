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
      //prepare
      session.sql("drop table if exists events.db.features")
      session.sql("create table events.db.features(user_id string, gender string, birthyear int, timestamp string, interested int, process_date string, event_id long) using iceberg")

      //run the pipeline
      runner.run(pipeline)(session)
    }
  }

  test(   "Pipeline test - iceberg read / file write") {
    for {
      session <- this.start()
      pipeline <- PipelineFactory.fromJson(loadContent(s"${resourceRoot}pipelines/pipeline_icebergRead-fileWrite.json"))(config, session)
    } ultimately {
      this.done(session)
    } {
      runner.run(pipeline)(session)
    }
  }

  override def createSparkSession(): SparkSession = SparkSession.builder()
    .appName("test")
    .master("local[*]")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
      //SparkSessionCatalog handles both iceberg and non-iceberg tables
      .config("spark.sql.catalog.spark_catalog", "org.apache.iceberg.spark.SparkSessionCatalog")
      .config("spark.sql.catalog.spark_catalog.type", "hive")
      //SparkCatalog handles only iceberg tables
      .config("spark.sql.catalog.events", "org.apache.iceberg.spark.SparkCatalog")
      .config("spark.sql.catalog.events.type", "hadoop")
      .config("spark.sql.catalog.events.warehouse", "/tmp/events-warehouse")
      //Spark’s default overwrite mode is static, but dynamic overwrite mode is recommended when writing to Iceberg tables.
      //Static overwrite mode determines which partitions to overwrite in a table by converting the PARTITION clause to a filter, but the PARTITION clause can only reference table columns.
      .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
    .getOrCreate()
}

