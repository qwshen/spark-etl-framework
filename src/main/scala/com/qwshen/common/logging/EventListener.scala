package com.qwshen.common.logging

class EventListener {

  import org.apache.spark.sql.execution.{QueryExecution, SQLExecution}
  val qu: QueryExecution = null
  qu.stringWithStats
}

/*
http://spark.apache.org/docs/2.3.1/api/java/org/apache/spark/sql/util/QueryExecutionListener.html

sc.addSparkListener(new SparkListener() {
  override def onApplicationStart(applicationStart: SparkListenerApplicationStart) {
    println("Spark ApplicationStart: " + applicationStart.appName);
  }

  override def onApplicationEnd(applicationEnd: SparkListenerApplicationEnd) {
    println("Spark ApplicationEnd: " + applicationEnd.time);
  }

});

 */


/*
 To register for the activity completion task, you need to derive your class from the StageCompletedEventConsumer trait. To register for the read count, you need to derive your class from the RecordsLoadedEventConsumer trait.
 To register for the write count, you need to derive your class from the RecordsWrittenEventConsumer trait.

 The read event presents an interesting situation. When Spark reads data, the read event is invoked twice - the first time after reading the first record and the second time after loading all the records. In other words, the read event listener is invoked with two values. The first time, the value of records read is one. The second time, the value of records read is the number of records in the data set.
 */

//import java.util.Properties
//
//2
//
//import org.apache.spark.SparkContext
//
//3
//
//import org.apache.spark.sql.{DataFrame, SparkSession, SQLContext}
//
//4
//
//import org.apache.spark.scheduler.{SparkListener, SparkListenerTaskEnd, SparkListenerStageCompleted}
//
//5
//
//import org.apache.spark.scheduler._
//
//6
//
//import scala.collection.mutable.
//
//7
//
//8
//
//val spatkContext = sc
//
//9
//
//val sparkSession = spark
//
//10
//
//val sqlContext = sparkSession.sqlContext
//
//11
//
//12
//
//trait StageCompletedEventConsumer {
//
//  13
//
//  def execute(executotRunTime: Long, executorCPUTime: Long)
//
//  14
//
//}
//
//15
//
//16
//
//class StageCompletionManager extends SparkListener
//
//17
//
//{
//
//18
//
//var consumerMap: scala.collection.mutable.Map[String, StageCompletedEventConsumer] = scala.collection.mutable.Map[String, StageCompletedEventConsumer)()
//
//19
//
//20
//
//def addEventConsumer(SparkContext: SparkContext, id: String, consumer: StageCompletedEventConsumer)
//
//21
//
//{
//
//22
//
//consumerMap += (id -> consumer)
//
//23
//
//}
//
//24
//
//25
//
//def removeEventConsumcr(id: String)
//
//26
//
//{
//
//27
//
//consumerMap -= id
//
//28
//
//}
//
//29
//
//30
//
//override def onStageCompleted(stageCompleted: SparkListenerStageCompleted): Unit =
//
//31
//
//{
//
//32
//
//for ( (k, v) <- consumerMap ) {
//
//33
//
//if ( v != null ) {
//
//34
//
//v.execute(stageCompleted.stageInfo.taskMetcics.executionRunTime, stageCompleted.stageInfo.taskMetcics.executorCpuTime)
//
//35
//
//}
//
//36
//
//}
//
//37
//
//}
//
//38
//
//}
//
//39
//
//40
//
//trait RecordsLoadedEventConsumer {
//
//  41
//
//  def execute(recordsRead: Long)
//
//  42
//
//}
//
//43
//
//44
//
//class RecordsLoadedManager extends SparkListener
//
//45
//
//{
//
//46
//
//var consumerMap: scala.collection.mutable.Map[String, RecordsLoadedEventConsumer] = scala.collection.mutable.Map[String, RecordsLoadedEventConsumer)()
//
//47
//
//48
//
//def addEventConsumer(SparkContext: SparkContext, id: String, consumer: RecordsLoadedEventConsumer)
//
//49
//
//{
//
//50
//
//consumerMap += (id -> consumer)
//
//51
//
//}
//
//52
//
//53
//
//def removeEventConsumer(id: String)
//
//54
//
//{
//
//55
//
//consumerMap -= id
//
//56
//
//}
//
//57
//
//58
//
//override def onTaskEnd(stageCompleted: SparkListenerTaskEnd): Unit =
//
//59
//
//{
//
//60
//
//val recordsRead = taskEnd.taskMetrics.inputMetrics.recordsRead
//
//61
//
//for ( (k, v) <- consumerMap ) {
//
//62
//
//if ( v != null ) {
//
//63
//
//v.execute(recordsRead)
//
//64
//
//}
//
//65
//
//}
//
//66
//
//}
//
//67
//
//}
//
//68
//
//69
//
//trait RecordsWrittenEventConsumer {
//
//  70
//
//  def execute(recordsWritten: Long)
//
//  71
//
//}
//
//72
//
//73
//
//class RecordsWrittenManager extends SparkListener
//
//74
//
//{
//
//75
//
//var consumerMap: scala.collection.mutable.Map[String, RecordsWrittenEventConsumer] = scala.collection.mutable.Map[String, RecordsWrittenEventConsumer)()
//
//76
//
//77
//
//def addEventConsumer(SparkContext: SparkContext, id: String, consumer: RecordsWrittenEventConsumer)
//
//78
//
//{
//
//79
//
//consumerMap += (id -> consumer)
//
//80
//
//}
//
//81
//
//82
//
//def removeEventConsumer(id: String)
//
//83
//
//{
//
//84
//
//consumerMap -= id
//
//85
//
//}
//
//86
//
//87
//
//override def onTaskEnd(stageCompleted: SparkListenerTaskEnd): Unit =
//
//88
//
//{
//
//89
//
//val recordsWritten = taskEnd.taskMetrics.outputMetrics.recordsWritten
//
//90
//
//for ( (k, v) <- consumerMap ) {
//
//91
//
//if ( v != null ) {
//
//92
//
//v.execute(recordsWritten)
//
//93
//
//}
//
//94
//
//}
//
//95
//
//}
//
//96
//
//}
//
//97
//
//98
//
//class Consumer1 extends RecordsLoadedEventConsumer
//
//99
//
//{
//
//100
//
//override def execute (recordsRead: Long) {
//
//101
//
//println("Consumer 1: " + recordsRead.toString)
//
//102
//
//}
//
//103
//
//}
//
//104
//
//105
//
//class Consumer2 extends RecordsLoadedEventConsumer
//
//106
//
//{
//
//107
//
//override def execute(recordsRead: Long) {
//
//108
//
//println("Consumer 2 : " + recordsRead.toString)
//
//109
//
//}
//
//110
//
//)
//
//111
//
//112
//
//class Consumer3 extends StageCompletedEventConsumer
//
//113
//
//{
//
//114
//
//override def execute(executorRunTime: Long, executorRunTime: Long)
//
//115
//
//{
//
//116
//
//println ("Consumer 3: " + executorRunTime.toString + ", " + executorCPUTime.tostring)
//
//117
//
//}
//
//118
//
//}
//
//119
//
//120
//
//val cl: Consumer1 = new Consumer1
//
//121
//
//val c2: Consumer2 = new Consumer2
//
//122
//
//val c3: Consumer3 = new Consumer3
//
//123
//
//124
//
//val rm: RecordsLoadedManager = new RecordsLoadedManager
//
//125
//
//sparkContext.addSparkListener(rm)
//
//126
//
//rm.addEventConsumer(sparkContext, "cl", c1)
//
//127
//
//rm.addEventConsumer(sparkContext, "c2", c2)
//
//128
//
//129
//
//val sm: StageCompletionManager = new StageCompletionManager
//
//130
//
//sparkContext.addSparkListene(sm)
//
//131
//
//sm.addEventConsumer(sparkContext, "c3", c3)
//
//132
//
//133
//
//val inputPath = "stations.csv"
//
//134
//
//val df = spackSession.read.format("csv").option("header". "true").option("sep", ",").option("inferSchema", "true").csv(inputPath)
//
//135
//
//136
//
//rm.removeEventConsuaer("c2")
//
//137
//
//138
//
//val df = sparkSession.read.format("csv").option("header", "true").option(sep, ",").option("inferSchema", "true").csv(inputPath)
//
//
