package com.qwshen.etl.transform

import com.qwshen.common.PropertyKey
import com.qwshen.etl.common.stream.ArbitraryStatefulProcessor
import com.qwshen.etl.common.{Actor, ExecutionContext}
import com.typesafe.config.Config
import org.apache.spark.sql.{DataFrame, SparkSession}
import scala.util.{Failure, Success, Try}

/**
 * Run a custom arbitrary stateful transformation with mapGroupsWithState or flatMapGroupsWithState.
 */
class StreamStatefulTransformer extends Actor {
  //the custom transform type which implements mapGroupsWithState or flatMapGroupsWithState
  private var _stateTransformer: Option[ArbitraryStatefulProcessor] = None

  //the source view
  @PropertyKey("view", true)
  protected var _sourceView: Option[String] = None

  /**
   * Run the transform
   *
   * @param ctx - the execution context
   * @param session - the spark-session
   *  @return
   */
  def run(ctx: ExecutionContext)(implicit session: SparkSession): Option[DataFrame] = for {
    transformer <- this._stateTransformer
    df <- this._sourceView.flatMap(name => ctx.getView(name))
  } yield Try {
    transformer.transformState(df)
  } match {
    case Success(ds) => ds
    case Failure(ex) => throw new RuntimeException("Process failed when running the custom stream-state transformer in StreamTransformer.", ex)
  }

  /**
   * Initialize the actor with the properties & config
   */
  override def init(properties: Seq[(String, String)], config: Config)(implicit session: SparkSession): Unit = {
    super.init(properties, config)

    //create the transformer
    this._stateTransformer = properties.find(_._1.equals("processor.type")).map(x => Class.forName(x._2).newInstance().asInstanceOf[ArbitraryStatefulProcessor])
      .map(x => {x.init(properties.filter(_._1.startsWith("processor.")).map(x => (x._1.replace("processor.", ""), x._2))); x})
    validate(this._stateTransformer, "The transformer-type in StreamStatefulTransformer is mandatory.")
  }

  /**
   * The type of the state-transformer
   * @param processor
   * @return
   */
  def transformer(processor: ArbitraryStatefulProcessor): StreamStatefulTransformer = { this._stateTransformer = Some(processor); this }

  /**
   * The name of the source view
   *
   * @param viewName
   * @return
   */
  def sourceView(viewName: String): StreamStatefulTransformer = { this._sourceView = Some(viewName); this }
}
