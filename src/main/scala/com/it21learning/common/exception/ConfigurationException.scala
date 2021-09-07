package com.it21learning.common.exception

case class ConfigurationException(message: String, t: Throwable) extends RuntimeException(message, t)
