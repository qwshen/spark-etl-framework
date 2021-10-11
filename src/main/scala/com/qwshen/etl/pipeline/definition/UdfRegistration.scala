package com.qwshen.etl.pipeline.definition

import com.qwshen.etl.common.UdfRegister

case class UdfRegistration(prefix: String, register: UdfRegister)