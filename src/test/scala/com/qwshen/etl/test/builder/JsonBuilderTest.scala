package com.qwshen.etl.test.builder

import com.qwshen.etl.pipeline.builder.JsonPipelineBuilder
import com.qwshen.etl.test.TestApp
import scala.util.control.Exception.ultimately

class JsonBuilderTest extends TestApp {
  test("json pipeline parser") {
    for (session <- this.start()) ultimately {
        done(session)
      } {
      val definition = loadContent(s"${resourceRoot}pipelines/template_pipeline.json")
      val pipeline = new JsonPipelineBuilder().build(definition)(config, session)
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
}
