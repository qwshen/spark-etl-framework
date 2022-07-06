package com.qwshen.etl.pipeline

import com.qwshen.common.logging.Loggable
import com.qwshen.etl.common.{JobContext, PipelineContext}
import com.qwshen.etl.pipeline.definition._
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.storage.StorageLevel
import org.apache.hadoop.fs.{FileSystem, Path}
import java.io.PrintWriter
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
import scala.collection.mutable.ArrayBuffer
import scala.util.{Failure, Success, Try}

final class PipelineRunner(appCtx: PipelineContext) extends Loggable {
  //to describe the content of metric entry
  private case class MetricEntry(jobName: String, actionName: String, key: String, value: String)

  /**
   * Execute the etl-pipeline
   *
   * @param pipeline
   * @param runJob
   * @param session
   */
  def run(pipeline: Pipeline, runJobs: Seq[String] = Nil)(implicit session: SparkSession): Unit = {
    val metrics = new ArrayBuffer[MetricEntry]()
    runJobs.foldLeft(pipeline.jobs)((jobs, runJob) => jobs.filter(job => job.name.equalsIgnoreCase(runJob.trim))).foreach(job => {
      //logging
      if (this.logger.isDebugEnabled()) {
        this.logger.info(s"Starting running job - ${job.name} ...")
      }
      //create a new session
      val curSession: SparkSession = if (pipeline.singleSparkSession) session else session.newSession()
      try {
        //create execution context
        val ctx: JobContext = new JobContext(appCtx, pipeline.config)(curSession)

        //register UDFs if any
        UdfRegistration.setup(pipeline.udfRegistrations)(curSession)
        //localize global views
        if (pipeline.globalViewAsLocal) {
          localizeGlobalViews(curSession)
        }

        //scan all referenced views
        val viewsReferenced: Map[String, Int] = scanReferencedViews(job)

        //execute jobs
        job.actions.foreach(action => {
          //logging
          if (this.logger.isDebugEnabled()) {
            this.logger.info(s"Starting running the action - ${action.name} ...")
          }
          //check if all referenced view exist
          ensureViewsExist(scanReferencedViews(action))(curSession)
          //check if metrics collection is required, so to give the hint to the actor before running
          ctx.metricsRequired = pipeline.metricsLogging.exists(ml => ml.loggingActions.exists(a => a.equalsIgnoreCase(action.name)))
          //execute
          action.actor.run(ctx)(curSession) collect { case r: DataFrame => r } foreach (df => {
            promoteView(df, action, pipeline.globalViewAsLocal, viewsReferenced)
            collectMetrics(job.name, action, pipeline.metricsLogging, df).foreach(me => metrics.append(me))
            stageView(df, action, pipeline.stagingBehavior)
          })
          //logging
          if (this.logger.isDebugEnabled()) {
            this.logger.info(s"Finished the action - ${action.name}.")
          }
        })

        //log metrics
        logMetrics(pipeline.name, metrics.toSeq, pipeline.metricsLogging)
      } finally {
        //clean up
        if (!pipeline.singleSparkSession) {
          discardSession(curSession)
        }
      }
      //logging
      if (this.logger.isDebugEnabled()) {
        this.logger.info(s"Finished the job - ${job.name}.")
      }
    })
  }

  //localize global views so they can be used without specifying the global database.
  private def localizeGlobalViews(implicit session: SparkSession): Unit = {
    import session.implicits._
    session.catalog.listTables(appCtx.global_db).filter('isTemporary).select('name)
      .collect.map(r => r.getString(0)).foreach(tbl => session.table(s"${appCtx.global_db}.$tbl").createOrReplaceTempView(tbl))
  }

  //scan all referenced views for the job
  private def scanReferencedViews(job: Job): Map[String, Int] = job.actions.flatMap(action => scanReferencedViews(action)).groupBy(x => x).map(x => (x._1, x._2.length))
  //scan all referenced view for the action
  private def scanReferencedViews(action: Action): Seq[String] = action.inputViews.union(action.actor.extraViews).distinct

  //to ensure all referenced views have been already created, otherwise error out.
  private def ensureViewsExist(views: Seq[String])(implicit session: SparkSession): Boolean = {
    val isSparkView = (view: String) => {
      val parts = view.split("\\.")
      if (parts.length > 2) false else if (parts.length < 2) true else if (parts(0).equals("global_temp")) true else false
    }

    views.filter(v => isSparkView(v)).map(v => (v, session.catalog.tableExists(v)))
      .foldLeft(true)(
        (r, v) => {
          if (!v._2) {
            throw new RuntimeException(s"The required view [${v._1}] doesn't exists. Please check the pipeline definition.")
          }
          r & v._2
        }
      )
  }

  //promote the data-frame as a view
  private def promoteView(df: DataFrame, action: Action, globalViewAsLocal: Boolean, viewsReferenced: Map[String, Int]): Unit = {
    action.output.foreach(v => {
      //if the view is global or referenced multiple times, cache it
      viewsReferenced.get(v.name) match {
        case Some(count) if (count > 1 && !df.isStreaming) => df.persist(StorageLevel.MEMORY_AND_DISK)
        case _ => if (v.global && !df.isStreaming) df.persist(StorageLevel.MEMORY_AND_DISK)
      }

      //create a global view if it is required
      if (v.global) {
        df.createOrReplaceGlobalTempView(v.name)
      }
      //create a local view if it is required.
      if (!v.global || globalViewAsLocal) {
        df.createOrReplaceTempView(v.name)
      }
    })
  }

  //collect matrics for the action
  private def collectMetrics(jobName: String, action: Action, metricsLogging: Option[MetricsLogging], df: DataFrame)(implicit session: SparkSession): Seq[MetricEntry] = Try {
    for {
      ml <- metricsLogging
      _ <- ml.loggingActions.find(a => a.equalsIgnoreCase(action.name)) if !df.isStreaming
    } yield {
      val customMetrics = action.actor.collectMetrics(df)
        .map(x => MetricEntry(jobName, action.name, x._1, x._2.replace("\"", "\\\"").replaceAll("[\r|\n| ]+", " ").replace("\r", "").replace("\n", " ")))

      if (!(df.storageLevel.useMemory || df.storageLevel.useDisk || df.storageLevel.useOffHeap)) {
        df.persist(StorageLevel.MEMORY_AND_DISK)
      }
      val systemMetrics = Seq(
        MetricEntry(jobName, action.name, "ddl-schema", df.schema.toDDL),
        MetricEntry(jobName, action.name, "row-count", df.count.toString),
        MetricEntry(jobName, action.name, "estimate-size", String.format("%s bytes", df.sparkSession.sessionState.executePlan(df.queryExecution.logical).optimizedPlan.stats.sizeInBytes.toString())),
        MetricEntry(jobName, action.name, "execute-time", LocalDateTime.now.toString)
      )
      systemMetrics ++ customMetrics
    }
  } match {
    case Success(r) => r.getOrElse(Nil)
    case Failure(t) => this.logger.warn(s"Cannot collect metrics for action - $jobName.${action.name}."); Nil
  }

  //write the metrics
  private def logMetrics(pipelineName: String, entries: Seq[MetricEntry], metricsLogging: Option[MetricsLogging])(implicit session: SparkSession): Unit = for {
    ml <- metricsLogging
    uri <- ml.loggingUri
  } Try {
    val jobsJsonString = entries.map(e => (e.jobName, e.actionName, String.format(""""%s": "%s"""", e.key, e.value)))
      .groupBy(_._1)
        .mapValues(v => v.map(x => (x._2, x._3)).groupBy(_._1).mapValues(v => "{ " + v.map(_._2).mkString(", ") + " }"))
        .mapValues(v => v.map(x => String.format("""{ "name": "%s", "metrics": %s }""", x._1, x._2)))
      .map(x => (String.format(""""name": "%s"""", x._1), """"actions": [""" + x._2.mkString(", ") + "]"))
      .map(x => "{ " + x._1 + ", " + x._2 + " }").mkString(", ")
    val targetFileUri = s"${uri.stripSuffix("/")}/${pipelineName.replace(" ", "_")}/metrics-" + LocalDateTime.now.format(DateTimeFormatter.ofPattern("yyyyMMddHHmmss")) + ".log"

    val writer = new PrintWriter(FileSystem.get(session.sparkContext.hadoopConfiguration).create(new Path(targetFileUri)))
    try {
      writer.write(s"""{ "pipeline-name": "$pipelineName", "jobs:": [ $jobsJsonString ] }""")
    } finally {
      writer.close()
    }
  } match {
    case Failure(t) => this.logger.warn(s"Cannot write metrics to target.", t)
    case _ =>
  }

  //stage the current view
  private def stageView(df: DataFrame, action: Action, stagingBehavior: Option[StagingBehavior]): Unit = Try {
    for {
      behavior <- stagingBehavior
      uri <- behavior.stagingUri if !df.isStreaming
    } {
      behavior.stagingActions.find(a => a.equalsIgnoreCase(action.name)).foreach(_ => {
        if (!(df.storageLevel.useMemory || df.storageLevel.useDisk || df.storageLevel.useOffHeap)) {
          df.persist(StorageLevel.MEMORY_AND_DISK)
        }
        val targetUri = s"${uri.stripSuffix("/")}/${action.name.replace(" ", "_")}"
        df.write.format("csv").mode("overwrite").option("header", "true").save(targetUri)
      })
    }
  } match {
    case Failure(t) => this.logger.warn(s"Cannot stage data to target.", t)
    case _ =>
  }

  //discard a spark-session by cleaning up its cache
  private def discardSession(session: SparkSession): Unit = {
    import session.implicits._
    //clean up all cached tables
    session.catalog.listTables.filter('isTemporary).select('name).collect.map(r => r.getString(0)).foreach(tbl => {
      if (session.catalog.isCached(tbl)) {
        session.catalog.uncacheTable(tbl)
      }
    })
    //clean up any catalog cache
    session.catalog.clearCache()
  }
}
