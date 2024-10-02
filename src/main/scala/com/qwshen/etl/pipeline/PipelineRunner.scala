package com.qwshen.etl.pipeline

import com.qwshen.common.logging.Loggable
import com.qwshen.etl.common.{JobContext, PipelineContext, SqlBase}
import com.qwshen.etl.pipeline.definition._
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.storage.StorageLevel
import org.apache.hadoop.fs.{FileSystem, Path}
import java.io.PrintWriter
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
import scala.collection.mutable.ArrayBuffer
import scala.util.{Failure, Success, Try}
import com.typesafe.config.Config

final class PipelineRunner(pc: PipelineContext) extends Loggable {
  //to describe the content of metric entry
  private case class MetricEntry(jobName: String, actionName: String, key: String, value: String)

  //check if it is a validation-run
  private val validationRun: Config => Option[Int] = (config) => Try(config.getString("application.runtime.validationRun")).toOption match {
    case Some(s) if s.equalsIgnoreCase("true") => Some(0)
    case Some(s) if s.equalsIgnoreCase("false") => None
    case Some(s) => Try(s.toInt).toOption match {
      case Some(n) => if (n >= 0) Some(n) else None
      case _ => None
    }
    case _ => None
  }

  /**
   * Execute the etl-pipeline
   *
   * @param pipeline - the pipeline object
   * @param runJobs - jobs from the pipeline to be run
   * @param session - the spark-session object
   */
  def run(pipeline: Pipeline, runJobs: Seq[String] = Nil)(implicit session: SparkSession): Unit = {
    val metrics = new ArrayBuffer[MetricEntry]()
    val dtStart = LocalDateTime.now.toString

    val validation = pipeline.config.flatMap(cfg => this.validationRun(cfg))
    runJobs.foldLeft(pipeline.jobs)((jobs, runJob) => jobs.filter(job => job.name.equalsIgnoreCase(runJob.trim))).foreach(job => {
      //logging
      if (this.logger.isDebugEnabled()) {
        this.logger.info(s"Starting running job - ${job.name} ...")
      }
      //create a new session
      val curSession: SparkSession = if (pipeline.singleSparkSession) session else session.newSession()
      //create execution context
      val ctx: JobContext = new JobContext(pc, pipeline.config)(curSession)
      try {
        //register UDFs if any
        UdfRegistration.setup(pipeline.udfRegistrations)(curSession)
        //localize global views
        if (pipeline.globalViewAsLocal) {
          localizeGlobalViews(curSession)
        }

        //execute jobs
        job.actions.foreach(action => {
          //logging
          if (this.logger.isDebugEnabled()) {
            this.logger.info(s"Starting running the action - ${action.name} ...")
          }
          //check if all referenced view exist
          ensureViewsExist(action.inputViews)(curSession)
          //flag all input views are referenced
          action.actor match {
            case _: SqlBase[_] =>
            case _ => action.inputViews.foreach(view => ctx.viewReferenced(view))
          }
          //check if metrics collection is required, so to give the hint to the actor before running
          ctx.metricsRequired = pipeline.metricsLogging.exists(ml => ml.loggingEnabled && ml.loggingActions.exists(a => a.equalsIgnoreCase(action.name)))
          //before run
          action.actor.beforeRun
          Try {
            action.actor.run(ctx)(curSession) collect { case r: DataFrame => validation.foldLeft(r)((v, n) => v.limit(n)) } foreach (df => {
              promoteView(df, action, pipeline.globalViewAsLocal)
              collectMetrics(job.name, action, pipeline.metricsLogging, Left(df)).foreach(me => metrics.append(me))
              stageView(df, action, ctx, pipeline.stagingBehavior)
            })
          } match {
            case Success(_) =>
            case Failure(t) =>
              collectMetrics(job.name, action, pipeline.metricsLogging, Right(t)).foreach(me => metrics.append(me))
              throw t
          }
          //logging
          if (this.logger.isDebugEnabled()) {
            this.logger.info(s"Finished the action - ${action.name}.")
          }
        })
      } finally {
        //log metrics
        logMetrics(pipeline.name, dtStart, LocalDateTime.now.toString, metrics, pipeline.metricsLogging)

        //dispose the context
        ctx.dispose()
        //clean up
        if (!pipeline.singleSparkSession) {
          cleanupSession(curSession)
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
    session.catalog.listTables(pc.global_db).filter('isTemporary).select('name)
      .collect.map(r => r.getString(0)).foreach(tbl => session.table(s"${pc.global_db}.$tbl").createOrReplaceTempView(tbl))
  }

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
  private def promoteView(df: DataFrame, action: Action, globalViewAsLocal: Boolean): Unit = {
    action.output.foreach(view => {
      //create a global view if it is required
      if (view.global) {
        df.createOrReplaceGlobalTempView(view.name)
      }
      //create a local view if it is required.
      if (!view.global || globalViewAsLocal) {
        df.createOrReplaceTempView(view.name)
      }
    })
  }

  //collect metrics for the action
  private def collectMetrics(jobName: String, action: Action, metricsLogging: Option[MetricsLogging], data: Either[DataFrame, Throwable])(implicit session: SparkSession): Seq[MetricEntry] = Try {
    for {
      ml <- metricsLogging
      _ <- ml.loggingActions.find(a => a.equalsIgnoreCase(action.name))
      if ml.loggingEnabled
    } yield {
      data match {
        case Left(df) if !df.isStreaming =>
          val customMetrics = action.actor.collectMetrics(Some(df))
            .map(x => MetricEntry(jobName, action.name, x._1, x._2.replace("\"", "\\\"").replaceAll("[\r|\n| ]+", " ").replace("\r", "").replace("\n", " ")))

          if (!(df.storageLevel.useMemory || df.storageLevel.useDisk || df.storageLevel.useOffHeap)) {
            df.persist(StorageLevel.MEMORY_AND_DISK)
          }
          Seq(
            MetricEntry(jobName, action.name, "ddl-schema", df.schema.toDDL),
            MetricEntry(jobName, action.name, "row-count", df.count.toString),
            MetricEntry(jobName, action.name, "estimate-size", String.format("%s bytes", df.sparkSession.sessionState.executePlan(df.queryExecution.logical).optimizedPlan.stats.sizeInBytes.toString())),
            MetricEntry(jobName, action.name, "execute-time", LocalDateTime.now.toString),
            MetricEntry(jobName, action.name, "status", "success")
          ) ++ customMetrics

        case Right(ex) =>
          val customMetrics = action.actor.collectMetrics(None)
            .map(x => MetricEntry(jobName, action.name, x._1, x._2.replace("\"", "\\\"").replaceAll("[\r|\n| ]+", " ").replace("\r", "").replace("\n", " ")))
          Seq(
            MetricEntry(jobName, action.name, "status", "failure"),
            MetricEntry(jobName, action.name, "error", ex.getMessage)
          ) ++ customMetrics

        case _ => Nil
      }
    }
  } match {
    case Success(r) => r.getOrElse(Nil)
    case Failure(t) => this.logger.warn(s"Cannot collect metrics for action - $jobName.${action.name} - [${t.getMessage}]."); Nil
  }

  //write the metrics
  private def logMetrics(pipelineName: String, dtStart: String, dtEnd: String, entries: Seq[MetricEntry], metricsLogging: Option[MetricsLogging])(implicit session: SparkSession): Unit = for {
    ml <- metricsLogging
    uri <- ml.loggingUri
    if ml.loggingEnabled
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
      writer.write(s"""{ "pipeline-name": "$pipelineName", "start-time": "$dtStart", "end-time": "$dtEnd", "jobs": [ $jobsJsonString ] }""")
    } finally {
      writer.close()
    }
  } match {
    case Failure(t) => this.logger.warn(s"Cannot write metrics to target.", t)
    case _ =>
  }

  //stage the current view
  private def stageView(df: DataFrame, action: Action, ctx: JobContext, stagingBehavior: Option[StagingBehavior]): Unit = Try {
    for {
      behavior <- stagingBehavior
      uri <- behavior.stagingUri
      if behavior.stagingEnabled && !df.isStreaming
    } {
      behavior.stagingActions.find(a => a.equalsIgnoreCase(action.name)).foreach(_ => {
        if (!(df.storageLevel.useMemory || df.storageLevel.useDisk || df.storageLevel.useOffHeap)) {
          df.persist(StorageLevel.MEMORY_AND_DISK)
        }
        val targetUri = s"${uri.stripSuffix("/")}/${action.name.replace(" ", "_")}"
        df.write.format("csv").mode("overwrite").option("header", "true").save(targetUri)
        action.actor.extraView.foreach(view =>
          ctx.getView(view).foreach(dfView => {
            if (!(dfView.storageLevel.useMemory || dfView.storageLevel.useDisk || dfView.storageLevel.useOffHeap)) {
              dfView.persist(StorageLevel.MEMORY_AND_DISK)
            }
            dfView.write.format("csv").mode("append").option("header", "true").save(targetUri)
          })
        )
      })
    }
  } match {
    case Failure(t) => this.logger.warn(s"Cannot stage data to target.", t)
    case _ =>
  }

  //clean up the cache and meta data of the spark-session
  private def cleanupSession(session: SparkSession): Unit = {
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
