//The AWS CLI has to be installed in the cluster and properly configured.

import sys.process._
import java.time.Instant
//Install using maven coordinates: com.jayway.jsonpath:json-path:2.4.0
import com.jayway.jsonpath.JsonPath

//Original input parameters.
val scaleFactor="1"
val instance="1"
val nStreams="1"
//Cluster configuration.
val nodes=2
val version="5.29.0"
val versionShort="529"
val autoTerminate="false"
//Run configuration.
val ts=Instant.now().getEpochSecond()
val tag=ts+"test"
val experimentName=s"sparkemr-${versionShort}-${nodes}nodes-${scaleFactor}gb-${tag}"
val dirNameWarehouse=s"tpcds-warehouse-sparkemr-${versionShort}-${scaleFactor}gb-${instance}-${tag}"
val dirNameResults="sparkemr-test"
val databaseName=s"tpcds_sparkemr_${versionShort}_${scaleFactor}gb_${instance}_${tag}"
val jarFile="/mnt/tpcds-jars/targetsparkdatabricks/client-1.2-SNAPSHOT-SHADED.jar"
val jobName=s"BSC-test ${tag}"
//Script operation flags.
val RUN_CREATE_CLUSTER=1

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
  args(14)="--all-or-query-file=query2.sql"

  // number of streams
  args(15)=s"--number-of-streams=${nStreams}"
  // flags (111111 schema|load|analyze|zorder|power|tput)
  args(16)="--execution-flags=111010"
  
  return args
}

//Create the cluster run the job.
doRun(RUN_CREATE_CLUSTER, databaseName, dirNameResults, experimentName, instance, scaleFactor, dirNameWarehouse,
      jarFile, nStreams, nodes, autoTerminate, version, jobName)

def jsonStringList(array : Array[String]) : String = {
  var retVal=""
  for ( i <- 0 to array.length - 1) { 
    retVal += ("\"" + array(i) + "\"")
    if( i < (array.length - 1) )
      retVal += ", "
  }
  return retVal
}

def ec2AttributesFunc() : String = {
  return s"""{
   "KeyName":"testalojakeypair",
   "InstanceProfile":"EMR_EC2_DefaultRole",
   "SubnetId":"subnet-01033078",
   "EmrManagedSlaveSecurityGroup":"sg-0d6c7aa7f3a231e50",
   "EmrManagedMasterSecurityGroup":"sg-0cefde07cc1a0a36e"
}
""".replace('\n', ' ')
}

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
      "ActionOnFailure":"TERMINATE_CLUSTER",
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

def instanceGroupsFunc(nodes : Int) : String = {
  return s"""[
   {
      "InstanceCount":${nodes},
      "InstanceGroupType":"CORE",
      "InstanceType":"i3.2xlarge",
      "Name":"Core - ${nodes}"
   },
   {
      "InstanceCount":1,
      "InstanceGroupType":"MASTER",
      "InstanceType":"i3.2xlarge",
      "Name":"Master - 1"
   }
]
""".replace('\n', ' ')
}

def configurationsFunc() : String = {
  return s"""[
   {
      "Classification":"spark-defaults",
      "Properties":{
         "spark.driver.memory":"5692M",
         "hive.exec.max.dynamic.partitions":"3000",
         "hive.exec.dynamic.partition.mode":"nonstrict",
         "spark.sql.broadcastTimeout":"7200",
         "spark.sql.crossJoin.enabled":"true"
      }
   }
]
""".replace('\n', ' ')
}

def bootstrapActionsFunc() : String = {
  return s"""[
   {
      "Path":"s3://bsc-bootstrap/s3fs/emr_init.sh",
      "Args":[
         "hadoop",
         "tpcds-jars,tpcds-results-test"
      ],
      "Name":"Custom action"
   }
]
""".replace('\n', ' ')
}

def createCreateClusterCmd(jarFile : String, paramsStr : String, nodes : Int, autoTerminate : String,
                          version : String, jobName : String) : String = {
  var autoTerminateStr = ""
  if( autoTerminate == "true" )
    autoTerminateStr = " --auto-terminate "
  val ec2Attributes = ec2AttributesFunc
  val steps = stepsFunc(jarFile, paramsStr)
  val instanceGroups = instanceGroupsFunc(nodes)
  val configurations = configurationsFunc
  val bootstrapActions = bootstrapActionsFunc
  return s"""
  aws emr create-cluster 
	--termination-protected 
	--applications Name=Hadoop Name=Hive Name=Spark 
	--ec2-attributes '${ec2Attributes}' 
	--release-label emr-${version} 
	--log-uri 's3n://bsc-emr-logs/' 
	--steps '${steps}' 
	--instance-groups '${instanceGroups}' 
	--configurations '${configurations}' ${autoTerminateStr} 
	--auto-scaling-role EMR_AutoScaling_DefaultRole 
	--bootstrap-actions '${bootstrapActions}' 
	--ebs-root-volume-size 10 
	--service-role EMR_DefaultRole 
	--enable-debugging 
	--name '${jobName}' 
	--scale-down-behavior TERMINATE_AT_TASK_COMPLETION 
	--region us-west-2
  
""".replace('\n', ' ')
}

def doRun(RUN_CREATE_CLUSTER : Int, databaseName : String, dirNameResults : String, experimentName : String,
                   instance : String, scaleFactor : String, dirNameWarehouse : String, jarFile : String, nStreams : String,
                    nodes : Int, autoTerminate : String, version : String, jobName : String) : Unit = {
  if( RUN_CREATE_CLUSTER == 1 ) {
    var args : Array[String] = createArgsArray(databaseName, dirNameResults, experimentName, instance, scaleFactor,
                                               dirNameWarehouse, jarFile, nStreams)
    var paramsStr = jsonStringList(args)
    var awsCLIcmd = createCreateClusterCmd(jarFile, paramsStr, nodes, autoTerminate, version, jobName)
    println(awsCLIcmd)
    var jsonClusterCreate = Seq("/bin/bash", "-c", awsCLIcmd).!!
    var cluster_id = JsonPath.read[String](jsonClusterCreate, "$.ClusterId")
    println("Created cluster with id: " + cluster_id)
  }
}

