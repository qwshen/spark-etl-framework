package com.qwshen.common.exception

case class ActorRuntimeException(name: String, message: String, t: Throwable) extends RuntimeException(s"Actor Name [$name] runtime error - $message", t)