package com.it21learning.etl.pipeline

import com.it21learning.common.VariableResolver
import com.it21learning.common.logging.Loggable
import com.it21learning.etl.ApplicationContext
import com.it21learning.etl.common.ExecutionContext
import com.it21learning.etl.configuration.ConfigurationManager
import com.it21learning.etl.pipeline.definition._
import com.it21learning.etl.setting.SqlVariableSetter
import com.it21learning.etl.transform.SqlTransformer
import com.typesafe.config.Config
import org.apache.spark.sql.{DataFrame, SparkSession}
import scala.util.{Failure, Try}

final class PipelineRunner(appCtx: ApplicationContext) extends Loggable {
  //staging behavior
  private var _stagingBehavior: Option[StagingBehavior] = None

  /**
   * Execute the etl-pipeline
   *
   * @param pipeline
   * @param session
   */
  def run(pipeline: Pipeline)(implicit session: SparkSession): Unit = {
    //set the staging behavior
    this._stagingBehavior = pipeline.stagingBehavior

    val config: Option[Config] = pipeline.config
    //run the jobs
    pipeline.jobs.foreach(job => {
      //logging
      if (this.logger.isDebugEnabled()) {
        this.logger.info(s"Starting running job - ${job.name} ...")
      }
      //create a new session
      val curSession: SparkSession = if (pipeline.singleSparkSession) session else session.newSession()
      try {
        //create execution context
        val ctx: ExecutionContext = new ExecutionContext(appCtx, config)(curSession)

        //register UDFs if any
        registerUDFs(pipeline.udfRegistrations)(curSession)
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
          //execute
          action.actor.run(ctx)(curSession) collect { case r: DataFrame => r } foreach(df => promoteView(df, action, pipeline.globalViewAsLocal, viewsReferenced))
          //logging
          if (this.logger.isDebugEnabled()) {
            this.logger.info(s"Finished the action - ${action.name}.")
          }
        })
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

  //register UDFs
  private def registerUDFs(udfRegisters: Seq[UdfRegistration])(implicit session: SparkSession): Unit = udfRegisters.foreach(r => r.register.register(r.prefix))

  //localize global views so they can be used without specifying the global database.
  private def localizeGlobalViews(implicit session: SparkSession): Unit = {
    import session.implicits._
    session.catalog.listTables(appCtx.global_db).filter('isTemporary).select('name)
      .collect.map(r => r.getString(0)).foreach(tbl => session.table(s"${appCtx.global_db}.$tbl").createOrReplaceTempView(tbl))
  }

  //scan all referenced views for the job
  private def scanReferencedViews(job: Job): Map[String, Int] = job.actions.flatMap(action => scanReferencedViews(action)).groupBy(x => x).map(x => (x._1, x._2.length))
  //scan all referenced view for the action
  private def scanReferencedViews(action: Action): Seq[String] = action.inputViews.union(action.actor.extraViews)

  //to ensure all referenced views have been already created, otherwise error out.
  private def ensureViewsExist(views: Seq[String])(implicit session: SparkSession): Boolean = views.map(v => (v, session.catalog.tableExists(v)))
    .foldLeft(true)(
      (r, v) => {
        if (!v._2) {
          throw new RuntimeException(s"The required view [${v._1}] doesn't exists. Please check the pipeline definition.")
        }
        r & v._2
      }
    )

  //promote the data-frame as a view
  private def promoteView(df: DataFrame, action: Action, globalViewAsLocal: Boolean, viewsReferenced: Map[String, Int]): Unit = {
    action.output.foreach(v => {
      //if the view is global or referenced multiple times, cache it
      viewsReferenced.get(v.name) match {
        case Some(count) if (count > 1 && !df.isStreaming) => df.cache
        case _ => if (v.global && !df.isStreaming) df.cache
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

    //staging
    Try {
      for {
        behavior <- this._stagingBehavior
        uri <- behavior.stagingUri
        if (!df.isStreaming)
      } {
        behavior.stagingActions.find(a => a == action.name).foreach(_ => {
          if (!(df.storageLevel.useMemory || df.storageLevel.useDisk || df.storageLevel.useOffHeap)) {
            df.cache
          }
          val targetUri = s"${uri.stripSuffix("/")}/${action.name.replace(" ", "_")}"
          df.write.format("csv").mode("overwrite").option("header", "true").save(targetUri)
        })
      }
    } match {
      case Failure(t) => this.logger.warn(s"Cannot stage data to target.", t)
      case _ =>
    }
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
