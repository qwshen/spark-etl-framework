package com.qwshen.etl.test.Pipeline

import com.qwshen.etl.pipeline.builder.PipelineFactory
import com.qwshen.etl.test.TestApp
import org.apache.spark.sql.SparkSession
import scala.util.control.Exception.ultimately

class DeltaPipelineTest extends TestApp {
  test("Pipeline test - file read / delta write") {
    for {
      session <- this.start()
      pipeline <- PipelineFactory.fromYaml(loadContent(s"${resourceRoot}pipelines/pipeline_fileRead-deltaWrite.yaml"))(config, session)
   } ultimately {
      this.done(session)
    } {
      runner.run(pipeline)(session)
    }
  }

  test("Pipeline test - delta read / file write") {
    for {
      session <- this.start()
      pipeline <- PipelineFactory.fromJson(loadContent(s"${resourceRoot}pipelines/pipeline_deltaRead-fileWrite.json"))(config, session)
    } ultimately {
      this.done(session)
    } {
      runner.run(pipeline)(session)
    }
  }

  test("Pipeline test - delta streaming read / kafka streaming write") {
    for {
      session <- this.start()
      pipeline <- PipelineFactory.fromXml(loadContent(s"${resourceRoot}pipelines/pipeline_deltaStreamRead-kafkaStreamWrite.xml"))(config, session)
    } ultimately {
      this.done(session)
    } {
      runner.run(pipeline)(session)
    }
  }

  test("Pipeline test - kafka streaming read / delta streaming write") {
    for {
      session <- this.start()
      pipeline <- PipelineFactory.fromYaml(loadContent(s"${resourceRoot}pipelines/pipeline_kafkaStreamRead-deltaStreamWrite.yaml"))(config, session)
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
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .getOrCreate()
}
