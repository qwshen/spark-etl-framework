package com.qwshen.etl.pipeline.definition

import com.qwshen.etl.common.Actor

case class Action(name: String, actor: Actor, output: Option[View] = None, inputViews: Seq[String] = Nil)
