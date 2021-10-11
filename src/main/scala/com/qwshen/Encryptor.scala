package com.qwshen

import com.qwshen.common.io.FileChannel
import com.qwshen.common.security.SecurityChannel
import scopt.OptionParser
import scala.util.{Success, Try}

object Encryptor {
  private case class Configuration(keyString: Option[String], keyFile: Option[String], dataString: String)

  def main(args: Array[String]): Unit = {
    Try(this.parse(args)) match {
      case Success(x) => System.out.println(new SecurityChannel(x._1).encrypt(x._2))
      case _ =>
        System.out.println("Usage: java -jar ./spark-etl-framework-1.0.jar com.qwshen.Encryptor --key-string key-value --data data-value")
        System.out.println("Usage: java -jar ./spark-etl-framework-1.0.jar com.qwshen.Encryptor --key-file file-name --data data-value")
    }
  }

  private val scoptParser: OptionParser[Configuration] = new OptionParser[Configuration]("spark-etl-framework") {
    head("spark-etl-framework")
    opt[String]("key-string")
      .optional()
      .action((x, c) => c.copy(keyString = Some(x)))
      .text("The key string")
    opt[String]("key-file")
      .optional()
      .action((x, c) => c.copy(keyFile = Some(x)))
      .text("The key file")
    opt[String]("data")
      .required()
      .action((x, c) => c.copy(dataString = x))
      .text("The text to be encrypted")
  }

  def parse(args: Array[String]): (String, String) = scoptParser.parse(args, Configuration(None, None, "")) match {
    case Some(cfg) => (cfg.keyString, cfg.keyFile) match {
      case (Some(ks), None) => (ks, cfg.dataString)
      case (None, Some(kf)) => (FileChannel.loadAsString(kf), cfg.dataString)
      case _ => throw new RuntimeException("Either the keyString or keyFile must be provided, but not both.")
    }
    case _ => throw new RuntimeException("Cannot parse the arguments.")
  }
}
