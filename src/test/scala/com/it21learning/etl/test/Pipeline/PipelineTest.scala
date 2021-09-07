package com.it21learning.etl.test.Pipeline

import java.net.URL
import com.it21learning.etl.ApplicationContext
import com.it21learning.etl.pipeline.PipelineRunner
import com.it21learning.etl.pipeline.builder.{PipelineFactory, XmlPipelineBuilder}
import com.it21learning.etl.test.{SparkApp, TestApp}
import com.typesafe.config.{Config, ConfigFactory}

import scala.io.Source
import scala.util.Properties

class PipelineTest extends SparkApp {
  implicit val config: Config = loadConfig()
  val runner = new PipelineRunner(new ApplicationContext())

  test("Pipeline test - file read / file write") {
    for (pipeline <- PipelineFactory.fromXml(loadContent(clsLoader.getResource("pipelines/pipeline_fileRead-fileWrite.xml")))) {
      runner.run(pipeline)
    }
  }

//  test("Pipeline test - file read / kafka write") {
//    implicit val config: Config = loadConfig()
//    val ctx = new ApplicationContext(config)
//
//    val pipeline = XmlTopology.fromString(loadContent(clsLoader.getResource("pipelines/pipeline_fileRead-kafkaWrite.xml")))
//    new PipelineRunner(ctx).run(pipeline)
//  }

//  test("Pipeline test - kafka read / file write") {
//    implicit val config: Config = loadConfig()
//    val ctx = new ApplicationContext(config)
//
//    val pipeline = XmlPipelineBuilder.fromString(loadContent(clsLoader.getResource("pipelines/pipeline_kafkaRead-fileWrite.xml")))
//    new PipelineRunner(ctx).run(pipeline)
//  }

//  test("Pipeline test - kafka stream-read / kafka stream-write") {
//    implicit val config: Config = loadConfig()
//    val ctx = new ApplicationContext(config)
//
//    val pipeline = XmlTopology.fromString(loadContent(clsLoader.getResource("pipelines/pipeline_kafkaStreamRead-kafkaStreamWrite.xml")))
//    new PipelineRunner(ctx).run(pipeline)
//  }

//  test("Pipeline test - file read / delta write") {
//    implicit val config: Config = loadConfig()
//    val ctx = new ApplicationContext(config)
//
//    val pipeline = XmlTopology.fromString(loadContent(clsLoader.getResource("pipelines/pipeline_fileRead-deltaWrite.xml")))
//    new PipelineRunner(ctx).run(pipeline)
//  }

//  test("Pipeline test - delta read / file write") {
//    implicit val config: Config = loadConfig()
//    val ctx = new ApplicationContext(config)
//
//    val pipeline = XmlTopology.fromString(loadContent(clsLoader.getResource("pipelines/pipeline_deltaRead-fileWrite.xml")))
//    new PipelineRunner(ctx).run(pipeline)
//  }

//  test("Pipeline test - delta streaming read / kafka streaming write") {
//    implicit val config: Config = loadConfig()
//    val ctx = new ApplicationContext(config)
//
//    val pipeline = XmlTopology.fromString(loadContent(clsLoader.getResource("pipelines/pipeline_deltaStreamRead-kafkaStreamWrite.xml")))
//    new PipelineRunner(ctx).run(pipeline)
//  }

//  test("Pipeline test - kafka streaming read / delta streaming write") {
//    implicit val config: Config = loadConfig()
//    val ctx = new ApplicationContext(config)
//
//    val pipeline = XmlTopology.fromString(loadContent(clsLoader.getResource("pipelines/pipeline_kafkaStreamRead-deltaStreamWrite.xml")))
//    new PipelineRunner(ctx).run(pipeline)
//  }

//  test("Pipeline test - file streaming read / file streaming write") {
//    implicit val config: Config = loadConfig()
//    val ctx = new ApplicationContext(config)
//
//    val pipeline = XmlTopology.fromString(loadContent(clsLoader.getResource("pipelines/pipeline_fileStreamRead-fileStreamWrite.xml")))
//    new PipelineRunner(ctx).run(pipeline)
//  }

//  test("Pipeline test - jdbc read / file write") {
//    implicit val config: Config = loadConfig()
//    val ctx = new ApplicationContext(config)
//
//    val pipeline = XmlTopology.fromString(loadContent(clsLoader.getResource("pipelines/pipeline_jdbcRead-fileWrite.xml")))
//    new PipelineRunner(ctx).run(pipeline)
//  }

//  test("Pipeline test - file read / jdbc write") {
//    implicit val config: Config = loadConfig()
//    val ctx = new ApplicationContext(config)
//
//    val pipeline = XmlTopology.fromString(loadContent(clsLoader.getResource("pipelines/pipeline_fileRead-jdbcWrite.xml")))
//    new PipelineRunner(ctx).run(pipeline)
//  }

//  test("Pipeline test - file stream read / jdbc stream write") {
//    implicit val config: Config = loadConfig()
//    val ctx = new ApplicationContext(config)
//
//    val pipeline = XmlTopology.fromString(loadContent(clsLoader.getResource("pipelines/pipeline_fileStreamRead-jdbcStreamWrite.xml")))
//    new PipelineRunner(ctx).run(pipeline)
//  }

//  test("Pipeline test - file stream read / arbitrary state / jdbc stream write") {
//    implicit val config: Config = loadConfig()
//    val ctx = new ApplicationContext(config)
//
//    val pipeline = XmlTopology.fromString(loadContent(clsLoader.getResource("pipelines/pipeline_fileStreamRead-arbitraryState-jdbcStreamWrite.xml")))
//    new PipelineRunner(ctx).run(pipeline)
//  }

//  test("Pipeline test - file read / hbase write") {
//    implicit val config: Config = loadConfig()
//    val ctx = new ApplicationContext(config)
//
//    val pipeline = XmlTopology.fromString(loadContent(clsLoader.getResource("pipelines/pipeline_fileRead-hbaseWrite.xml")))
//    new PipelineRunner(ctx).run(pipeline)
//  }

//  test("Pipeline test - file streaming read / hbase streaming write") {
//    implicit val config: Config = loadConfig()
//    val ctx = new ApplicationContext(config)
//
//    val pipeline = XmlTopology.fromString(loadContent(clsLoader.getResource("pipelines/pipeline_fileStreamRead-hbaseStreamWrite.xml")))
//    new PipelineRunner(ctx).run(pipeline)
//  }

//  test("Pipeline test - hbase read / file write") {
//    implicit val config: Config = loadConfig()
//    val ctx = new ApplicationContext(config)
//
//    val pipeline = XmlTopology.fromString(loadContent(clsLoader.getResource("pipelines/pipeline_hbaseRead-fileWrite.xml")))
//    new PipelineRunner(ctx).run(pipeline)
//  }

//  test("Pipeline test - file read / redis write") {
//    implicit val config: Config = loadConfig()
//    val ctx = new ApplicationContext(config)
//
//    val pipeline = XmlTopology.fromString(loadContent(clsLoader.getResource("pipelines/pipeline_fileRead-redisWrite.xml")))
//    new PipelineRunner(ctx).run(pipeline)
//  }

//  test("Pipeline test - file stream read / redis stream write") {
//    implicit val config: Config = loadConfig()
//    val ctx = new ApplicationContext(config)
//
//    val pipeline = XmlTopology.fromString(loadContent(clsLoader.getResource("pipelines/pipeline_fileStreamRead-redisStreamWrite.xml")))
//    new PipelineRunner(ctx).run(pipeline)
//  }

//  test("Pipeline test - redis read / file write") {
//    implicit val config: Config = loadConfig()
//    val ctx = new ApplicationContext(config)
//
//    val pipeline = XmlTopology.fromString(loadContent(clsLoader.getResource("pipelines/pipeline_redisRead-fileWrite.xml")))
//    new PipelineRunner(ctx).run(pipeline)
//  }

//  test("Pipeline test - redis stream read / file stream write") {
//    implicit val config: Config = loadConfig()
//    val ctx = new ApplicationContext(config)
//
//    val pipeline = XmlTopology.fromString(loadContent(clsLoader.getResource("pipelines/pipeline_redisStreamRead-fileStreamWrite.xml")))
//    new PipelineRunner(ctx).run(pipeline)
//  }

}
