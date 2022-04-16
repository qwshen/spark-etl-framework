package com.qwshen.etl.test.Pipeline

import com.qwshen.etl.test.TestApp
import org.apache.spark.sql.SparkSession

class IcebergPipelineTest extends TestApp {
  test("Pipeline test - file read / iceberg write") {
    val prepare = (session: SparkSession) => {
      session.sql("drop table if exists events.db.features")
      session.sql("create table events.db.features(user_id string, gender string, birthyear int, timestamp string, interested int, process_date string, event_id long) using iceberg partitioned by (gender, interested)")
      ()
    }
    this.run(s"${resourceRoot}pipelines/pipeline_fileRead-icebergWrite.xml", Some(prepare))
  }

  test(   "Pipeline test - iceberg read / file write") {
    this.run(s"${resourceRoot}pipelines/pipeline_icebergRead-fileWrite.json")
  }

  test(   "Pipeline test - iceberg streaming read / file streaming write") {
    this.run(s"${resourceRoot}pipelines/pipeline_icebergStreamRead-fileStreamWrite.json")
  }

  test("Pipeline test - file streaming read / iceberg streaming write") {
    val prepare = (session: SparkSession) => {
      session.sql("drop table if exists events.db.features")
      session.sql("create table events.db.features(user_id string, gender string, birthyear int, timestamp string, interested int, process_date string) using iceberg partitioned by (gender, interested)")
      ()
    }
    this.run(s"${resourceRoot}pipelines/pipeline_fileStreamRead-icebergStreamWrite.json", Some(prepare))
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

