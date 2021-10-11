package com.qwshen.common.exception

case class ConfigurationException(message: String, t: Throwable) extends RuntimeException(message, t)
