package com.it21learning.etl.transform

import com.it21learning.common.PropertyKey
import com.it21learning.etl.common.stream.ArbitraryStatefulProcessor
import com.it21learning.etl.common.{Actor, ExecutionContext}
import com.typesafe.config.Config
import org.apache.spark.sql.{DataFrame, SparkSession}
import scala.util.{Failure, Success, Try}
import scala.xml.NodeSeq

/**
 * Run a custom arbitrary stateful transformation with mapGroupsWithState or flatMapGroupsWithState.
 *
 * The following is the xml definition.
 *
 * <actor type="com.it21learning.etl.transform.StreamStatefulTransformer">
 *   <!-- the type of the arbitrary state transformer which must implement ArbitraryStateProcessor -->
 *   <processor type="com.it21learning.etl.transform.EventTransform">
 *     <!-- the following lists all properties the transformer requires to perform its job -->
 *     <property name="timeoutType">ProcessingTimeTimeout|EventTimeTimeout|NoTimeout</property>
 *     <property name="timeoutDuration">27 minutes</property>
 *   </processor>
 *   <!-- the source view the transformer manipulates -->
 *   <property name="view">events</property>
 * </actor>
 */
final class StreamStatefulTransformer extends Actor {
  //the custom transform type which implements mapGroupsWithState or flatMapGroupsWithState
  private var _stateTransformer: Option[ArbitraryStatefulProcessor] = None

  //the source view
  @PropertyKey("view", false)
  private var _sourceView: Option[String] = None

  /**
   * Run the transform
   *
   * @param ctx - the execution context
   * @param session - the spark-session
   *  @return
   */
  def run(ctx: ExecutionContext)(implicit session: SparkSession): Option[DataFrame] = for {
    _ <- validate(this._stateTransformer, "The transformer-type in StreamTransformer is mandatory.")
    transformer <- this._stateTransformer
    _ <-validate(this._sourceView, "The source-view in StreamTransformer is mandatory.")
    df <- this._sourceView.flatMap(name => ctx.getView(name))
  } yield Try {
    transformer.transformState(df)
  } match {
    case Success(ds) => ds
    case Failure(ex) => throw new RuntimeException("Error'ed when running the custom stream-state transformer in StreamTransformer.", ex)
  }

  /**
   * Initialize the SqlTransform
   *
   * @param definition
   * @param config - the configuration object
   * @param session
   */
  override def init(definition: NodeSeq, config: Config)(implicit session: SparkSession): Unit = {
    super.init(definition, config)

    (definition \ "processor").headOption.foreach(dp => {
      this._stateTransformer = Try(Class.forName((dp \ "@type").text).newInstance().asInstanceOf[ArbitraryStatefulProcessor]) match {
        case Success(transformer) => {
          transformer.init(dp, config)
          Some(transformer)
        }
        case Failure(exception) => throw new RuntimeException("The type of the state transformer in StreamStatefulTransformer is invalid.", exception)
      }
    })
    validate(this._stateTransformer, "The transformer-type in StreamStatefulTransformer is mandatory.")
    validate(this._sourceView, "The source-view in StreamStatefulTransformer is mandatory.")
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
