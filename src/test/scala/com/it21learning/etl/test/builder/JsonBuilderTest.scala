package com.it21learning.etl.test.builder

import com.it21learning.etl.pipeline.builder.JsonPipelineBuilder
import com.it21learning.etl.test.{SparkApp, TestApp}
import com.typesafe.config.Config

class JsonBuilderTest extends SparkApp {
  test("json pipeline parser") {
    implicit val config: Config = loadConfig()
    val definition = loadContent(clsLoader.getResource("pipelines/template_pipeline.json"))
    val pipeline = new JsonPipelineBuilder().build(definition)
    assert(pipeline.nonEmpty)
    pipeline.foreach(pl => {
      assert(pl.singleSparkSession)
      assert(pl.globalViewAsLocal)

      assert(pl.config.exists(cfg => cfg.getString("iam_password") == "hadoop"))
      assert(pl.config.exists(cfg => cfg.getString("process_date") == "2021-02-06"))

      assert(pl.jobs.count(j => j.name == "transform-user-events") == 2)
      pl.jobs.foreach(j => assert(j.actions.length == 3))
    })
  }
}
