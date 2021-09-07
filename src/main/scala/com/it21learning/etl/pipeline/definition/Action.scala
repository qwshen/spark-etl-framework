package com.it21learning.etl.pipeline.definition

import com.it21learning.etl.common.Actor

case class Action(name: String, actor: Actor, output: Option[View] = None, inputViews: Seq[String] = Nil)
