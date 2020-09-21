//The AWS CLI has to be installed in the cluster and properly configured.

import sys.process._
import java.time.Instant
//Install using maven coordinates: com.jayway.jsonpath:json-path:2.4.0
import com.jayway.jsonpath.JsonPath

//Id of the cluster to add the step to.
val clusterId="j-28K117NJYK39X"
val tag="1590952676test"
val nodes=2
val versionShort="529"
//Original input parameters.
val scaleFactor="1"
val instance="2"
val nStreams="1"
//Run configuration.
val experimentName=s"sparkemr-${versionShort}-${nodes}nodes-${scaleFactor}gb-${tag}"
val dirNameWarehouse=s"tpcds-warehouse-sparkemr-${versionShort}-${scaleFactor}gb-${instance}-${tag}"
val dirNameResults="sparkemr-test"
val databaseName=s"tpcds_sparkemr_${versionShort}_${scaleFactor}gb_${instance}_${tag}"
val jarFile="/mnt/tpcds-jars/targetsparkdatabricks/client-1.2-SNAPSHOT-SHADED.jar"

def createArgsArray(databaseName : String, dirNameResults : String, experimentName : String,
                   instance : String, scaleFactor : String, dirNameWarehouse : String,
                   jarFile : String, nStreams : String) : Array[String] = {

  var args = new Array[String](17)
  // main work directory
  args(0)="--main-work-dir=/data"
  // schema (database) name
  args(1)=s"--schema-name=${databaseName}"
  // results folder name (e.g. for Google Drive)
  args(2)=s"--results-dir=${dirNameResults}"
  // experiment name (name of subfolder within the results folder)
  args(3)=s"--experiment-name=${experimentName}"
  // system name (system name used within the logs)
  args(4)="--system-name=sparkemr"

  // experiment instance number
  args(5)=s"--instance-number=${instance}"
  // prefix of external location for raw data tables (e.g. S3 bucket), null for none
  args(6)=s"--ext-raw-data-location=s3://tpcds-datasets/${scaleFactor}GB"
  // prefix of external location for created tables (e.g. S3 bucket), null for none
  args(7)=s"--ext-tables-location=s3://tpcds-warehouses-test/${dirNameWarehouse}"
  // format for column-storage tables (PARQUET, DELTA)
  args(8)="--table-format=parquet"
  // whether to use data partitioning for the tables (true/false)
  args(9)="--use-partitioning=false"

  // "all" or create table file
  args(10)="--all-or-create-file=all"
  // jar file
  args(11)=s"--jar-file=${jarFile}"
  // whether to generate statistics by analyzing tables (true/false)
  args(12)="--use-row-stats=true"
  // if argument above is true, whether to compute statistics for columns (true/false)
  args(13)="--use-column-stats=true"
  // "all" or query file
  args(14)="--all-or-query-file=query27.sql"

  // number of streams
  args(15)=s"--number-of-streams=${nStreams}"
  // flags (111111 schema|load|analyze|zorder|power|tput)
  args(16)="--execution-flags=000010"
  
  return args
}

//Add the step to the cluster.
doRun(clusterId, databaseName, dirNameResults, experimentName, instance, scaleFactor, dirNameWarehouse, jarFile, nStreams)

def jsonStringList(array : Array[String]) : String = {
  var retVal=""
  for ( i <- 0 to array.length - 1) { 
    retVal += ("\"" + array(i) + "\"")
    if( i < (array.length - 1) )
      retVal += ", "
  }
  return retVal
}

//Options for action on failure: TERMINATE_CLUSTER, CANCEL_AND_WAIT, and CONTINUE
def stepsFunc(jarFile : String, paramsStr : String) : String = {
  return s"""[
   {
      "Args":[
         "spark-submit",
         "--deploy-mode",
         "client",
         "--conf",
         "spark.eventLog.enabled=true",
         "--class",
         "org.bsc.dcc.vcv.RunBenchmarkSparkCLI",
         "${jarFile}",
         ${paramsStr}
      ],
      "Type":"CUSTOM_JAR",
      "ActionOnFailure":"CANCEL_AND_WAIT",
      "Jar":"command-runner.jar",
      "Properties":"",
      "Name":"Spark application"
   }
]
""".replace('\n', ' ')
}

def stepsFuncHudi(jarFile : String, paramsStr : String) : String = {
  return s"""[
   {
      "Args":[
         "spark-submit",
         "--deploy-mode",
         "client",
         "--conf",
         "spark.eventLog.enabled=true",
         "--conf",
         "spark.serializer=org.apache.spark.serializer.KryoSerializer",
         "--conf",
         "spark.sql.hive.convertMetastoreParquet=false",
		 "--jars",
		 "/usr/lib/hudi/hudi-spark-bundle.jar,/usr/lib/spark/external/lib/spark-avro.jar",
         "--class",
         "org.bsc.dcc.vcv.RunBenchmarkSparkCLI",
         "${jarFile}",
         ${paramsStr}
      ],
      "Type":"CUSTOM_JAR",
      "ActionOnFailure":"TERMINATE_CLUSTER",
      "Jar":"command-runner.jar",
      "Properties":"",
      "Name":"Spark application"
   }
]
""".replace('\n', ' ')
}

def addStepToClusterCmd(jarFile : String, paramsStr : String, clusterId : String) : String = {
  val steps = stepsFunc(jarFile, paramsStr)
  return s"""
    aws emr add-steps --cluster-id ${clusterId} --steps '${steps}'
""".replace('\n', ' ')
}

def doRun(clusterId : String, databaseName : String, dirNameResults : String, experimentName : String, instance : String, scaleFactor : String,
          dirNameWarehouse : String, jarFile : String, nStreams : String) : Unit = {
  var args : Array[String] = createArgsArray(databaseName, dirNameResults, experimentName, instance, scaleFactor,
                                               dirNameWarehouse, jarFile, nStreams)
  var paramsStr = jsonStringList(args)
  var awsCLIcmd = addStepToClusterCmd(jarFile, paramsStr, clusterId)
  println(awsCLIcmd)
  var jsonClusterAddStep = Seq("/bin/bash", "-c", awsCLIcmd).!!
  var step_id = JsonPath.read[String](jsonClusterAddStep, "$.StepIds[0]")
  println("Added to the cluster a step with id: " + step_id)
}

