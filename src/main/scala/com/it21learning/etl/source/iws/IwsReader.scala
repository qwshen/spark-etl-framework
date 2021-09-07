//package com.it21learning.etl.source
//
//import org.apache.spark.sql.SparkSession
//import org.apache.spark.sql.functions._
//import scala.collection.mutable.ArrayBuffer
//
//class IwsReader {
//  def load(fileUri: String)(implicit session: SparkSession): Unit = {
//    import session.implicits._
//
//    val dfIWS = session.read.textFile(fileUri).rdd.zipWithIndex.toDF("message", "seqno")
//      .withColumn("flag", when('message.startsWith("MSGSTART"), 1)).cache
//
//    val maxSeqNo = dfIWS.count - 1
//
//    val dfSeqNo = dfIWS.filter('flag.isNotNull).select('seqno, 'flag).rdd.zipWithIndex.map(r => (r._1.getLong(0), r._1.getInt(1), r._2))
//      .toDF("seqno", "flag", "seqno_flag").cache
//    val dfStart = dfSeqNo.select('seqno.as("start_seqno"), 'seqno_flag)
//    val dfEnd = dfSeqNo.select(('seqno - 1).as("end_seqno"), ('seqno_flag - 1).as("seqno_flag"))
//
//    val dfAlignment = dfStart.as("s").join(dfEnd.as("e"), $"s.seqno_flag" === $"e.seqno_flag", "left_outer")
//      .select($"start_seqno", when($"end_seqno".isNull, maxSeqNo).otherwise($"end_seqno"), $"s.seqno_flag".as("msg_no")).cache
//
//    val dfMessages = dfIWS.alias("o").join(dfAlignment.alias("a"), expr("o.seqno >= a.start_seqno and o.seqno <= a.end_seqno"), "left_outer")
//      .filter($"msg_no".isNotNull).select("message", "msg_no")
//      .groupBy("msg_no").agg(collect_list($"message").as("message"))
//      .withColumn("message", concat_ws(scala.util.Properties.lineSeparator, $"message"))
//      .cache
//
//    //split iws message into seq(("I", msg), ("O", msg))
//    val splitMsg = udf {
//      (msg: String) => {
//        val result = msg.split("[\r|\n]+").foldLeft((("", new ArrayBuffer[String]()), new ArrayBuffer[(String, String)]))(
//          (r, s) => {
//            if (s.startsWith("INCOMING")) {
//              r._2.append((r._1._1, r._1._2.mkString(scala.util.Properties.lineSeparator)))
//              val t = ArrayBuffer[String](s)
//              (("I", t), r._2)
//            }
//            else if (s.startsWith("OUTGOING")) {
//              r._2.append((r._1._1, r._1._2.mkString(scala.util.Properties.lineSeparator)))
//              val t = ArrayBuffer[String](s)
//              (("O", t), r._2)
//            }
//            else {
//              r._1._2.append(s)
//              ((r._1._1, r._1._2), r._2)
//            }
//          }
//        )
//        result._2.append((result._1._1, result._1._2.mkString(scala.util.Properties.lineSeparator)))
//
//        val pattern = "(TO\\s+\\d+\\s+\\d+)\\s+(\\d{3})\\s+(COMP CODE)?(SEND STATUS)?:(.*)\\s+LENGTH:".r
//        result._2.filter(x => x._1 != "").map(
//          x => pattern.findFirstIn(x._2) match {
//            case Some(s) => s match {
//              case pattern(to_number, sub_number, _, _, code_status) => Seq(x._1, to_number, sub_number, code_status, x._2)
//              case _ => Seq(x._1, "", "", "", x._2)
//            }
//            case _ => Seq(x._1, "", "", "", x._2)
//          }
//        )
//      }
//    }
//
//    val dfDetails = dfMessages.withColumn("message", explode(splitMsg($"message")))
//      .withColumn("msg_direction", $"message".getItem(0))
//      .withColumn("to_number", $"message".getItem(1)).withColumn("sub_number", $"message".getItem(2)).withColumn("code_status", $"message".getItem(3))
//      .withColumn("msg_body", $"message".getItem(4))
//      .drop("message")
//
//    def search(msg: String, word: String): Seq[Int] = {
//      val indexes = new ArrayBuffer[Int]()
//      var idx = msg.indexOf(word)
//      while (idx >= 0) {
//        indexes.append(idx)
//        idx = msg.indexOf(word, idx + 1)
//      }
//      indexes
//    }
//    val parseSwift = udf {
//      (msg: String) => {
//        val startIdx = msg.indexOf("MESSAGE TEXT")
//        if (startIdx >= 0) {
//          val indexes: Seq[(String, Int)] = Seq("{1:", "{2:", "{3:", "{4:", "{5:").map(x => (x, search(msg, x))).filter(x => x._2.nonEmpty).map(x => (x._1, x._2(0)))
//
//          val endIndex = msg.indexOf("OPERATOR HISTORY")
//          val endPos = if (endIndex >= 0) endIndex else msg.length - 1
//          val msgHolder: ArrayBuffer[(String, String)] = new ArrayBuffer[(String, String)]()
//          for (i <- indexes.indices) {
//            val body = if (i < indexes.length - 1) msg.substring(indexes(i)._2, indexes(i + 1)._2) else msg.substring(indexes(i)._2, endPos)
//            msgHolder.append((indexes(i)._1, body))
//          }
//
//          val s1: String = msgHolder.filter(x => x._1 == "{1:").map(x => x._2).headOption.getOrElse("")
//          val s2: String = msgHolder.filter(x => x._1 == "{2:").map(x => x._2).headOption.getOrElse("")
//          val s3: String = msgHolder.filter(x => x._1 == "{3:").map(x => x._2).headOption.getOrElse("")
//          val s4: String = msgHolder.filter(x => x._1 == "{4:").map(x => x._2).headOption.getOrElse("")
//          val s5: String = msgHolder.filter(x => x._1 == "{5:").map(x => x._2).headOption.getOrElse("")
//          Seq(s1, s2, s3, s4, s5)
//        }
//        else {
//          Seq("", "", "", "", "")
//        }
//      }
//    }
//
//    val dfSwifts = dfDetails.withColumn("swifts", parseSwift($"msg_body"))
//      .withColumn("swift_1", $"swifts".getItem(0))
//      .withColumn("swift_2", $"swifts".getItem(1))
//      .withColumn("swift_3", $"swifts".getItem(2))
//      .withColumn("swift_4", $"swifts".getItem(3))
//      .withColumn("swift_5", $"swifts".getItem(4)).drop("swifts")
//
//    val parseMessageType = udf {
//      (swift_2: String) => {
//        val pattern = "\\{2:[O|I](\\d{3})".r.unanchored
//        swift_2 match {
//          case pattern(msg_type) => msg_type
//          case _ => ""
//        }
//      }
//    }
//
//    val dfMsgType = dfSwifts.withColumn("msg_type", parseMessageType($"swift_2"))
//      .select("msg_no", "msg_direction", "msg_type", "to_number", "sub_number", "code_status", "swift_1", "swift_2", "swift_3", "swift_4", "swift_5", "msg_body")
//  }
//}
