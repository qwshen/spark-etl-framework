package com.qwshen.etl.test.Pipeline

import com.qwshen.etl.test.TestApp

class PipelineTest extends TestApp {
  test("Pipeline test - file read / file write") {
    this.run(s"${resourceRoot}pipelines/pipeline_fileRead-fileWrite.xml")
  }

  test("Pipeline test - file read / kafka write") {
    this.run(s"${resourceRoot}pipelines/pipeline_fileRead-kafkaWrite.yaml")
  }

  test("Pipeline test - kafka read / file write") {
    this.run(s"${resourceRoot}pipelines/pipeline_kafkaRead-fileWrite.json")
  }

  test("Pipeline test - kafka stream-read / kafka stream-write") {
    this.run(s"${resourceRoot}pipelines/pipeline_kafkaStreamRead-kafkaStreamWrite.xml")
  }

  test("Pipeline test - file streaming read / file streaming write") {
    this.run(s"${resourceRoot}pipelines/pipeline_fileStreamRead-fileStreamWrite.json")
  }

  test("Pipeline test - jdbc read / file write") {
    this.run(s"${resourceRoot}pipelines/pipeline_jdbcRead-fileWrite.json")
  }

  test("Pipeline test - file read / jdbc write") {
    // ***Note: please create the train table first by using create-train.sql in a MySQL instance before running this test case.
    this.run(s"${resourceRoot}pipelines/pipeline_fileRead-jdbcWrite.yaml")
  }

  test("Pipeline test - file stream read / jdbc stream write") {
    // ***Note: please create the features table first by using create-features.sql in a MySQL instance before running this test case.
    this.run(s"${resourceRoot}pipelines/pipeline_fileStreamRead-jdbcStreamWrite.json")
  }

  test("Pipeline test - file read / hbase write") {
    // ***Note: please create the users table first by using hbase_setup.txt in a HBase instance before running this test case.
    this.run(s"${resourceRoot}pipelines/pipeline_fileRead-hbaseWrite.yaml")
  }

  test("Pipeline test - file streaming read / hbase streaming write") {
    // ***Note: please create the users table first by using hbase_setup.txt in a HBase instance before running this test case.
    this.run(s"${resourceRoot}pipelines/pipeline_fileStreamRead-hbaseStreamWrite.xml")
  }

  test("Pipeline test - hbase read / file write") {
    this.run(s"${resourceRoot}pipelines/pipelines/pipeline_hbaseRead-fileWrite.yaml")
  }

  test("Pipeline test - file read / redis write") {
    this.run(s"${resourceRoot}pipelines/pipeline_fileRead-redisWrite.json")
  }

  test("Pipeline test - file stream read / redis stream write") {
    this.run(s"${resourceRoot}pipelines/pipeline_fileStreamRead-redisStreamWrite.xml")
  }

  test("Pipeline test - redis read / file write") {
    this.run(s"${resourceRoot}pipelines/pipeline_redisRead-fileWrite.yaml")
  }

  test("Pipeline test - redis stream read / file stream write") {
    this.run(s"${resourceRoot}pipelines/pipeline_redisStreamRead-fileStreamWrite.xml")
  }

  test("Pipeline test - file stream read / arbitrary state / jdbc stream write") {
    //Note: please run mysql_setup.sh before running this test case
    this.run(s"${resourceRoot}pipelines/pipeline_fileStreamRead-arbitraryState-jdbcStreamWrite.yaml")
  }
}
