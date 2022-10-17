package com.qbeast.opensourcedev

object Main extends App {
  println("Creating test analytics log.")
  val workDir = "/tmp"
  val resultsDir = "dbr81-simple-write"
  val experimentName = "tpcds-databricks-81-1000gb-1617206310"
  val system = "sparkdatabricks"
  val test = "writetest"
  val instance = 1
  val recorder = new AnalyticsRecorder(workDir, resultsDir, experimentName, system, test, instance)

  recorder.createWriter()
  recorder.header()
  val query1 = new QueryRecord(1,0,0,0,false,0,10)
  recorder.record(query1)
  val query2 = new QueryRecord(2,0,0,0,false,0,5)
  recorder.record(query2)
  recorder.close()
  println("Test log created in: " + recorder.getLogFilePath())
}