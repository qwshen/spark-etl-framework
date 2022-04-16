package com.qwshen.etl.test.Pipeline

import com.qwshen.etl.test.TestApp
import org.apache.spark.sql.SparkSession

class MongoPipelineTest extends TestApp {
  test("Pipeline test - file read / mongo write") {
    this.run(s"${resourceRoot}pipelines/pipeline_fileRead-mongoWrite.json")
  }

  test("Pipeline test - mongo read / file write") {
    this.run(s"${resourceRoot}pipelines/pipeline_mongoRead-fileWrite.yaml")
  }

  override def createSparkSession(): SparkSession = SparkSession.builder()
    .appName("test")
    .master("local[*]")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .config("spark.mongodb.input.uri", "mongodb://localhost:27017/events.users")
      .config("spark.mongodb.output.uri", "mongodb://localhost:27017/events.train")
    .getOrCreate()
}

