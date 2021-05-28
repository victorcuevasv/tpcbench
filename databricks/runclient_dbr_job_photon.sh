#!/bin/bash

#Variables for console output with colors.

red=$'\e[1;31m'
grn=$'\e[1;32m'
yel=$'\e[1;33m'
blu=$'\e[1;34m'
mag=$'\e[1;35m'
cyn=$'\e[1;36m'
end=$'\e[0m'

#$1 scale factor (positive integer)
#$2 experiment instance number (positive integer)
#$3 number of streams (positive integer)

USE_DBR_CLI=1

if [ -z "$DATABRICKS_TOKEN" ] && [ "$USE_DBR_CLI" -eq 0 ] ; then
    echo "${yel}The environment variable DATABRICKS_TOKEN is not defined.${end}"
    exit 0
fi

if [ $# -lt 3 ]; then
    echo "${yel}Usage: bash runclient_dbr_job.sh <scale factor> <experiment instance number> <number of streams>${end}"
    exit 0
fi

printf "\n\n%s\n\n" "${mag}Running the TPC-DS benchmark.${end}"

#Cluster configuration.
DatabricksHost="dbc-08fc9045-faef.cloud.databricks.com"
Nodes="16"
MajorVersion="8"
MinorVersion="3p"
ScalaVersion="x-scala2.12"
#Run configuration.
Tag="$(date +%s)"
ExperimentName="tpcds-databricks-${MajorVersion}${MinorVersion}-$1gb-${Tag}-$3streams"
DirNameWarehouse="tpcds-databricks-${MajorVersion}${MinorVersion}-$1gb-$2-${Tag}"
DirNameResults="dbr${MajorVersion}${MinorVersion}"
DatabaseName="tpcds_databricks_${MajorVersion}${MinorVersion}_$1gb_$2_${Tag}"
#Note that /dbfs or dbfs: is not included, these are added later.
JarFile="/mnt/tpcds-jars/targetsparkdatabricks/client-1.2-SNAPSHOT-SHADED.jar"
JOB_NAME="Run TPC-DS Benchmark ${Tag} $2"
#Script operation flags.
RUN_CREATE_JOB=1
RUN_RUN_JOB=1
WAIT_FOR_TERMINATION=0
RUN_DELETE_WAREHOUSE=0

args=()
# main work directory
args[0]="--main-work-dir=/data"
# schema (database) name
args[1]="--schema-name=$DatabaseName"
# results folder name (e.g. for Google Drive)
args[2]="--results-dir=$DirNameResults"
# experiment name (name of subfolder within the results folder)
args[3]="--experiment-name=$ExperimentName"
# system name (system name used within the logs)
args[4]="--system-name=sparkdatabricks"

# experiment instance number
args[5]="--instance-number=$2"
# prefix of external location for raw data tables (e.g. S3 bucket), null for none
args[6]="--ext-raw-data-location=dbfs:/mnt/tpcds-datasets/$1GB"
# prefix of external location for created tables (e.g. S3 bucket), null for none
args[7]="--ext-tables-location=dbfs:/mnt/tpcds-warehouses-test/$DirNameWarehouse"
# format for column-storage tables (PARQUET, DELTA)
args[8]="--table-format=delta"
# whether to use data partitioning for the tables (true/false)
args[9]="--use-partitioning=true"

# jar file
args[10]="--jar-file=/dbfs${JarFile}"
# whether to generate statistics by analyzing tables (true/false)
args[11]="--use-row-stats=true"
# if argument above is true, whether to compute statistics for columns (true/false)
args[12]="--use-column-stats=true"
# "all" or query file
args[13]="--all-or-query-file=all" 
# number of streams
args[14]="--number-of-streams=$3"

# flags (111111 schema|load|analyze|zorder|power|tput)
args[15]="--execution-flags=111011"
# "all" or create table file
args[16]="--all-or-create-file=all"

printf "\n\n%s\n\n" "${mag}Creating the job.${end}"

function json_string_list() {
    declare array=("$@")
    declare list=""
    for w in "${array[@]}"
    do
        list+="\"$w\", "
    done
    #Remove the last comma and space
    echo ${list%??}
}

paramsStr=$(json_string_list "${args[@]}")

#"spark_conf":{
#        	"spark.databricks.delta.optimize.maxFileSize":"134217728",
#            "spark.databricks.delta.optimize.minFileSize":"134217728",
#            "spark.sql.crossJoin.enabled":"true",
#            "spark.databricks.optimizer.deltaTableFilesThreshold":"100",
#            "spark.sql.broadcastTimeout":"7200",
#            "spark.databricks.delta.autoCompact.maxFileSize":"134217728",
#            "hive.exec.dynamic.partition.mode":"nonstrict",
#            "hive.exec.max.dynamic.partitions":"3000"
#         }
post_data_func()
{
  cat <<EOF
{ 
	"name":"$JOB_NAME",
	"new_cluster":{ 
		"spark_version":"custom:release__8.3.x-snapshot-photon-scala2.12__databricks-universe__head__fd0ce79__6b751c3__jenkins__65df5fa__format-2.lz4",
        "spark_conf":{
            "spark.databricks.photon.enabled":"true",
            "spark.memory.offHeap.enabled":"true",
            "spark.memory.offHeap.size":"36000m",
            "spark.executor.memory":"8000m",
            "spark.databricks.execution.adaptiveParallelism.enabled":"false",
            "spark.sql.shuffle.partitions":"256"
         },
         "aws_attributes":{ 
            "zone_id":"us-west-2b",
            "instance_profile_arn": "arn:aws:iam::384416317380:instance-profile/ShardS3Access_SSE-2051",
            "availability":"ON_DEMAND"
         },
         "node_type_id":"i3.2xlarge",
         "driver_node_type_id": "i3.8xlarge",
         "ssh_public_keys":[ 
            "ssh-rsa AAAAB3NzaC1yc2EAAAADAQABAAABAQDjxM9WRCj8vI0aDAVhVi3DkibIaChqwjshgcPvXcth5l1cWxBZLkDdL1CCLSAbUGGL+HX79FL7L46wBCOHTJ2hhd7tjxxDG7IeH/G2Q1wPsTMt7Vpswc8Ijp6BKzbqAJS9HJAq9VPjh0x39gPd2x4vHiRpudKA+RFvTQ1jWRz0nTxI/eteZWB03jrPQxbZFo5v/29VDTwRlDBraC5q3hblfXAUk8cWQkhlFz2XagPNzsigVsY/zJvIJ/zW5ZpPI5El2VK3CEGjqbg5qt7QnIiRydly3N2eWHDIwZM3nfAMYdWig+65U8LOy9NC8J6dk8v/ZlstoOLNNm5+LSkmj9b7 pristine@al-1001"   
         ],
         "enable_elastic_disk":false,
         "num_workers":$Nodes
      },
      "libraries":[ 
         { 
            "jar":"dbfs:$JarFile"
         }
      ],
      "email_notifications":{ 

      },
      "timeout_seconds":0,
      "spark_jar_task":{ 
         "jar_uri":"",
         "main_class_name":"org.bsc.dcc.vcv.RunBenchmarkSparkCLI",
         "parameters":[ 
			$paramsStr
         ]
      },
      "max_concurrent_runs":1
}
EOF
}

#External metastore script:
#dbfs:/databricks/init_scripts/set_spark_embedded_metastore.sh

#Get the state (RUNNING, TERMINATED) of a job run via its run_id using the Jobs REST API.
#$1 run_id
get_run_state() {
	declare jsonStr=""
	if [ "$USE_DBR_CLI" -eq 0 ] ; then
		jsonStr=$(curl -s -n \
   		-H "Authorization: Bearer $DATABRICKS_TOKEN" \
   		https://${DatabricksHost}/api/2.0/jobs/runs/get?run_id=$1)
	else
		jsonStr=$(databricks runs get --run-id $1)
	fi
	declare state=$(jq -j '.state.life_cycle_state'  <<<  "$jsonStr")
	echo $state
}

#Wait until the state of a job run is TERMINATED by polling using the Jobs REST API.
#$1 run_id
#$2 pause in seconds for polling
wait_for_run_termination() {
   declare state=$(get_run_state $1)
   while [ $state != "TERMINATED"  ] ;
   do
      sleep $2
      state=$(get_run_state $1)
   done
}

#Create a job using the Jobs REST API.
create_job() {
   declare jsonStr=$(curl -s -X POST \
	-H "Authorization: Bearer $DATABRICKS_TOKEN" \
	-H "Content-Type: application/json" \
	-d "$(post_data_func)" \
	https://${DatabricksHost}/api/2.0/jobs/create)
	#Example output.
	#{"job_id":236}
   declare state=$(jq -j '.job_id'  <<<  "$jsonStr")
   echo $state
}

#Run a job using the Jobs REST API.
#$1 id of the job to run.
run_job() {
   declare jsonStr=$(curl -s -X POST \
	-H "Authorization: Bearer $DATABRICKS_TOKEN" \
	-H "Content-Type: application/json" \
	-d "{ \"job_id\": $1 }" \
	https://${DatabricksHost}/api/2.0/jobs/run-now)
   echo $jsonStr
}

job_id=""
job_run_id=""

if [ "$RUN_CREATE_JOB" -eq 1 ]; then
	echo "${blu}Creating job for benchmark execution.${end}"
	if [ "$USE_DBR_CLI" -eq 0 ]; then
		job_id=$(create_job)
	else
		#Must add the quotes to post_data_func to avoid error.
		jsonJobCreate=$(databricks jobs create --json "$(post_data_func)")
		job_id=$(jq -j '.job_id'  <<<  "$jsonJobCreate")
	fi
	echo "${blu}Created job with id ${job_id}.${end}"
fi

if [ "$RUN_RUN_JOB" -eq 1 ]; then
	echo "${blu}Running job with id ${job_id}.${end}"
	jsonJobRun=""
	if [ "$USE_DBR_CLI" -eq 0 ]; then
		jsonJobRun=$(run_job $job_id)
	else
		jsonJobRun=$(databricks jobs run-now --job-id $job_id)
	fi
	job_run_id=$(jq -j '.run_id' <<< "$jsonJobRun")
fi

if [ "$WAIT_FOR_TERMINATION" -eq 1 ]; then
	echo "${blu}Waiting for the completion of job with run id ${job_run_id}.${end}"
	wait_for_run_termination $job_run_id 120
	echo "${blu}Benchmark running job completed.${end}"
	if [ "$RUN_DELETE_WAREHOUSE" -eq 1 ]; then
    		echo "${blu}Deleting the warehouse directory ${DirNameWarehouse}.${end}"
    		aws s3 rm --recursive s3://tpcds-warehouses-test/$DirNameWarehouse
	fi
fi


