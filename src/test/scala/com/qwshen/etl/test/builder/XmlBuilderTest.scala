package com.qwshen.etl.test.builder

import com.qwshen.etl.pipeline.builder.XmlPipelineBuilder
import com.qwshen.etl.test.{SparkApp, TestApp}
import com.typesafe.config.Config

class XmlBuilderTest extends SparkApp {
  test("xml pipeline parser") {
    implicit val config: Config = loadConfig()
    val definition = loadContent(s"${resourceRoot}pipelines/template_pipeline.xml")
    val pipeline = new XmlPipelineBuilder().build(definition)
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
