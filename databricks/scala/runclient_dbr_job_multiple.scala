//The Databricks CLI has to be installed in the cluster and properly configured.

import sys.process._
import java.time.Instant
//Install using maven coordinates: com.jayway.jsonpath:json-path:2.4.0
import com.jayway.jsonpath.JsonPath

def createArgsArray(jobName : String, majorVersion : String, minorVersion : String,
          scalaVersion : String, nodes : String, jarFile : String, 
          databaseName : String, dirNameResults : String, experimentName : String, instance : String,
          scaleFactor : String, dirNameWarehouse : String, nStreams : String) : Array[String] = {

  var args = new Array[String](17)
  //main work directory
  args(0)="--main-work-dir=/data"
  //schema (database) name
  args(1)=s"--schema-name=${databaseName}"
  //results folder name (e.g. for Google Drive)
  args(2)=s"--results-dir=${dirNameResults}"
  //experiment name (name of subfolder within the results folder)
  args(3)=s"--experiment-name=${experimentName}"
  //system name (system name used within the logs)
  args(4)="--system-name=sparkdatabricks"

  //experiment instance number
  args(5)=s"--instance-number=${instance}"
  //prefix of external location for raw data tables (e.g. S3 bucket), null for none
  args(6)=s"--ext-raw-data-location=dbfs:/mnt/tpcdsbucket/${scaleFactor}GB"
  //prefix of external location for created tables (e.g. S3 bucket), null for none
  args(7)=s"--ext-tables-location=dbfs:/mnt/tpcds-warehouses-test/${dirNameWarehouse}"
  //format for column-storage tables (PARQUET, DELTA)
  args(8)="--table-format=parquet"
  //whether to use data partitioning for the tables (true/false)
  args(9)="--use-partitioning=false"

  //jar file
  args(10)=s"--jar-file=/dbfs${jarFile}"
  //whether to generate statistics by analyzing tables (true/false)
  args(11)="--use-row-stats=true"
  //if argument above is true, whether to compute statistics for columns (true/false)
  args(12)="--use-column-stats=true"
  //"all" or query file
  args(13)="--all-or-query-file=query2.sql" 
  //number of streams
  args(14)=s"--number-of-streams=${nStreams}"

  //flags (111111 schema|load|analyze|zorder|power|tput)
  args(15)="--execution-flags=110000"
  //"all" or create table file
  args(16)="--all-or-create-file=call_center.sql"

  return args
}

//Create and run the jobs.
doRun()

def jsonStringList(array : Array[String]) : String = {
  var retVal=""
  for ( i <- 0 to array.length - 1) { 
    retVal += ("\"" + array(i) + "\"")
    if( i < (array.length - 1) )
      retVal += ", "
  }
  return retVal
}

def postDataFunc(jobName : String, majorVersion : String, minorVersion : String, scalaVersion : String,
                nodes : String, jarFile : String, paramsStr : String) : String = {
  return s"""{ 
	"name":"${jobName}",
	"new_cluster":{ 
		"spark_version":"${majorVersion}.${minorVersion}.${scalaVersion}",
        "spark_conf":{
        	"spark.databricks.delta.optimize.maxFileSize":"134217728",
            "spark.databricks.delta.optimize.minFileSize":"134217728",
            "spark.sql.crossJoin.enabled":"true",
            "spark.databricks.optimizer.deltaTableFilesThreshold":"100",
            "spark.sql.broadcastTimeout":"7200",
            "spark.databricks.delta.autoCompact.maxFileSize":"134217728",
            "hive.exec.dynamic.partition.mode":"nonstrict",
            "hive.exec.max.dynamic.partitions":"3000"
         },
         "aws_attributes":{ 
            "zone_id":"us-west-2b",
            "availability":"ON_DEMAND"
         },
         "node_type_id":"i3.2xlarge",
         "ssh_public_keys":[ 
            "ssh-rsa AAAAB3NzaC1yc2EAAAADAQABAAABAQDjxM9WRCj8vI0aDAVhVi3DkibIaChqwjshgcPvXcth5l1cWxBZLkDdL1CCLSAbUGGL+HX79FL7L46wBCOHTJ2hhd7tjxxDG7IeH/G2Q1wPsTMt7Vpswc8Ijp6BKzbqAJS9HJAq9VPjh0x39gPd2x4vHiRpudKA+RFvTQ1jWRz0nTxI/eteZWB03jrPQxbZFo5v/29VDTwRlDBraC5q3hblfXAUk8cWQkhlFz2XagPNzsigVsY/zJvIJ/zW5ZpPI5El2VK3CEGjqbg5qt7QnIiRydly3N2eWHDIwZM3nfAMYdWig+65U8LOy9NC8J6dk8v/ZlstoOLNNm5+LSkmj9b7 pristine@al-1001"   
         ],
         "enable_elastic_disk":false,
         "num_workers":${nodes}
      },
      "libraries":[ 
         { 
            "jar":"dbfs:${jarFile}"
         }
      ],
      "email_notifications":{ 

      },
      "timeout_seconds":0,
      "spark_jar_task":{ 
         "jar_uri":"",
         "main_class_name":"org.bsc.dcc.vcv.RunBenchmarkSparkCLI",
         "parameters":[ 
			${paramsStr}
         ]
      },
      "max_concurrent_runs":1
}""".replace('\n', ' ')
}

def doRun() : Unit = {
  
  //Original input parameters.
  val scaleFactor=1
  val instances=2
  val nStreams=1
  //Cluster configuration.
  val nodes="2"
  val majorVersion="6"
  val minorVersion="5"
  val scalaVersion="x-scala2.11"
  //Run configuration.
  val ts=Instant.now().getEpochSecond()
  val tag=ts+"test"
  val experimentName=s"tpcds-databricks-${majorVersion}${minorVersion}-${scaleFactor}gb-${tag}"
  val dirNameResults="databricks-test"
  //Note that /dbfs or dbfs: is not included, these are added later.
  val jarFile="/mnt/tpcds-jars/targetsparkdatabricks/client-1.2-SNAPSHOT-SHADED.jar"
  //Script operation flags (0/1).
  val RUN_CREATE_JOB=1
  val RUN_RUN_JOB=1
  
  for( instance <- 1 to instances) {
    
    var dirNameWarehouse=s"tpcds-databricks-${majorVersion}${minorVersion}-${scaleFactor}gb-${instance}-${tag}"
    var databaseName=s"tpcds_databricks_${majorVersion}${minorVersion}_${scaleFactor}gb_${instance}_${tag}"
    var jobName=s"Run TPC-DS Benchmark ${tag} ${instance}"

    if( RUN_CREATE_JOB == 1 ) {
      var args : Array[String] = createArgsArray(jobName, majorVersion, minorVersion, scalaVersion, nodes, jarFile,
                                                 databaseName, dirNameResults, experimentName, instance + "", scaleFactor + "", 
                                                 dirNameWarehouse, nStreams + "")
      var paramsStr=jsonStringList(args)
      var postData=postDataFunc(jobName, majorVersion, minorVersion, scalaVersion, nodes, jarFile, paramsStr)
      var createCmd = "databricks jobs create --json '" + postData + "'"
      println(createCmd)
      var jsonJobCreate = Seq("/bin/bash", "-c", createCmd).!!
      var job_id = JsonPath.read[Int](jsonJobCreate, "$.job_id")
      println("Created job with id: " + job_id)
      if( RUN_RUN_JOB == 1 ) {
        var createCmd = s"databricks jobs run-now --job-id ${job_id}"
        var jsonJobRun = Seq("/bin/bash", "-c", createCmd).!!
        var run_id = JsonPath.read[Int](jsonJobRun, "$.run_id")
        println("Running job with id: " + job_id + " with run_id: " + run_id)
      }
    }
  }
}

