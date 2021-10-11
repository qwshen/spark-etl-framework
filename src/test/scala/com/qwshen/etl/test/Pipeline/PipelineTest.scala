package com.qwshen.etl.test.Pipeline

import com.qwshen.etl.ApplicationContext
import com.qwshen.etl.pipeline.PipelineRunner
import com.qwshen.etl.pipeline.builder.PipelineFactory
import com.qwshen.etl.test.SparkApp
import com.typesafe.config.Config

class PipelineTest extends SparkApp {
  implicit val config: Config = loadConfig()
  val runner = new PipelineRunner(new ApplicationContext())

  test("Pipeline test - file read / file write") {
    for (pipeline <- PipelineFactory.fromXml(loadContent(s"${resourceRoot}pipelines/pipeline_fileRead-fileWrite.xml"))) {
      runner.run(pipeline)
    }
  }

  test("Pipeline test - file read / kafka write") {
    for (pipeline <- PipelineFactory.fromYaml(loadContent(s"${resourceRoot}pipelines/pipeline_fileRead-kafkaWrite.yaml"))) {
      runner.run(pipeline)
    }
  }

  test("Pipeline test - kafka read / file write") {
    for (pipeline <- PipelineFactory.fromJson(loadContent(s"${resourceRoot}pipelines/pipeline_kafkaRead-fileWrite.json"))) {
      runner.run(pipeline)
    }
  }

  test("Pipeline test - kafka stream-read / kafka stream-write") {
    for (pipeline <- PipelineFactory.fromXml(loadContent(s"${resourceRoot}pipelines/pipeline_kafkaStreamRead-kafkaStreamWrite.xml"))) {
      runner.run(pipeline)
    }
  }

  test("Pipeline test - file read / delta write") {
    for (pipeline <- PipelineFactory.fromYaml(loadContent(s"${resourceRoot}pipelines/pipeline_fileRead-deltaWrite.yaml"))) {
      runner.run(pipeline)
    }
  }

  test(   "Pipeline test - delta read / file write") {
    for (pipeline <- PipelineFactory.fromJson(loadContent(s"${resourceRoot}pipelines/pipeline_deltaRead-fileWrite.json"))) {
      runner.run(pipeline)
    }
  }

  test("Pipeline test - delta streaming read / kafka streaming write") {
    for (pipeline <- PipelineFactory.fromXml(loadContent(s"${resourceRoot}pipelines/pipeline_deltaStreamRead-kafkaStreamWrite.xml"))) {
      runner.run(pipeline)
    }
  }

  test("Pipeline test - kafka streaming read / delta streaming write") {
    for (pipeline <- PipelineFactory.fromYaml(loadContent(s"${resourceRoot}pipelines/pipeline_kafkaStreamRead-deltaStreamWrite.yaml"))) {
      runner.run(pipeline)
    }
  }

  test("Pipeline test - file streaming read / file streaming write") {
    for (pipeline <- PipelineFactory.fromJson(loadContent(s"${resourceRoot}pipelines/pipeline_fileStreamRead-fileStreamWrite.json"))) {
      runner.run(pipeline)
    }
  }

  test("Pipeline test - jdbc read / file write") {
    for (pipeline <- PipelineFactory.fromJson(loadContent(s"${resourceRoot}pipelines/pipeline_jdbcRead-fileWrite.json"))) {
      runner.run(pipeline)
    }
  }

  test("Pipeline test - file read / jdbc write") {
    // ***Note: please create the train table first by using create-train.sql in a MySQL instance before running this test case.
    for (pipeline <- PipelineFactory.fromYaml(loadContent(s"${resourceRoot}pipelines/pipeline_fileRead-jdbcWrite.yaml"))) {
      runner.run(pipeline)
    }
  }

  test("Pipeline test - file stream read / jdbc stream write") {
    // ***Note: please create the features table first by using create-features.sql in a MySQL instance before running this test case.
    for (pipeline <- PipelineFactory.fromJson(loadContent(s"${resourceRoot}pipelines/pipeline_fileStreamRead-jdbcStreamWrite.json"))) {
      runner.run(pipeline)
    }
  }

  test("Pipeline test - file read / hbase write") {
    // ***Note: please create the users table first by using hbase_setup.txt in a HBase instance before running this test case.
    for (pipeline <- PipelineFactory.fromYaml(loadContent(s"${resourceRoot}pipelines/pipeline_fileRead-hbaseWrite.yaml"))) {
      runner.run(pipeline)
    }
  }

  test("Pipeline test - file streaming read / hbase streaming write") {
    // ***Note: please create the users table first by using hbase_setup.txt in a HBase instance before running this test case.
    for (pipeline <- PipelineFactory.fromXml(loadContent(s"${resourceRoot}pipelines/pipeline_fileStreamRead-hbaseStreamWrite.xml"))) {
      runner.run(pipeline)
    }
  }

  test("Pipeline test - hbase read / file write") {
    for (pipeline <- PipelineFactory.fromYaml(loadContent(s"${resourceRoot}pipelines/pipelines/pipeline_hbaseRead-fileWrite.yaml"))) {
      runner.run(pipeline)
    }
  }

  test("Pipeline test - file read / redis write") {
    for (pipeline <- PipelineFactory.fromJson(loadContent(s"${resourceRoot}pipelines/pipeline_fileRead-redisWrite.json"))) {
      runner.run(pipeline)
    }
  }

  test("Pipeline test - file stream read / redis stream write") {
    for (pipeline <- PipelineFactory.fromXml(loadContent(s"${resourceRoot}pipelines/pipeline_fileStreamRead-redisStreamWrite.xml"))) {
      runner.run(pipeline)
    }
  }

  test("Pipeline test - redis read / file write") {
    for (pipeline <- PipelineFactory.fromYaml(loadContent(s"${resourceRoot}pipelines/pipeline_redisRead-fileWrite.yaml"))) {
      runner.run(pipeline)
    }
  }

  test("Pipeline test - redis stream read / file stream write") {
    for (pipeline <- PipelineFactory.fromXml(loadContent(s"${resourceRoot}pipelines/pipeline_redisStreamRead-fileStreamWrite.xml"))) {
      runner.run(pipeline)
    }
  }

  test("Pipeline test - file stream read / arbitrary state / jdbc stream write") {
    //Note: please run mysql_setup.sh before running this test case
    for (pipeline <- PipelineFactory.fromYaml(loadContent(s"${resourceRoot}pipelines/pipeline_fileStreamRead-arbitraryState-jdbcStreamWrite.yaml"))) {
      runner.run(pipeline)
    }
  }
}
