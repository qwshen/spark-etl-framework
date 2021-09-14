package com.it21learning.etl.test.builder

import com.it21learning.etl.pipeline.builder.YamlPipelineBuilder
import com.it21learning.etl.test.{SparkApp, TestApp}
import com.typesafe.config.Config

class YamlBuilderTest extends SparkApp {
  test("yaml pipeline parser") {
    implicit val config: Config = loadConfig()
    val definition = loadContent(s"${resourceRoot}pipelines/template_pipeline.yaml")
    val pipeline = new YamlPipelineBuilder().build(definition)
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
