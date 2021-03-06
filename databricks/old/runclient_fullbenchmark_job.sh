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

if [ $# -lt 3 ]; then
    echo "${yel}Usage: bash runclient_fullbenchmark_job.sh <scale factor> <experiment instance number> <number of streams>${end}"
    exit 0
fi

printf "\n\n%s\n\n" "${mag}Running the full TPC-DS benchmark.${end}"

Nodes="2"
MajorVersion="6"
MinorVersion="4"
Tag="experimental"
DirNameWarehouse="tpcds-warehouse-sparkdatabricks-${MajorVersion}${MinorVersion}-$1gb-$2${Tag}"
DirNameResults="1odwczxc3jftmhmvahdl7tz32dyyw0pen"
DatabaseName="tpcds_warehouse_sparkdatabricks_${MajorVersion}${MinorVersion}_$1gb_$2${Tag}"
DirNameExperiment="tpcds-sparkdatabricks-${MajorVersion}${MinorVersion}-$1gb-$2${Tag}"

printf "\n\n%s\n\n" "${mag}Creating the job.${end}"

JOB_NAME="Run TPC-DS Benchmark"

args=()

#args[0] main work directory
args[0]="/data"
#args[1] schema (database) name
args[1]="$DatabaseName"
#args[2] results folder name (e.g. for Google Drive)
args[2]="$DirNameResults"
#args[3] experiment name (name of subfolder within the results folder)
args[3]="$DirNameExperiment"
#args[4] system name (system name used within the logs)
args[4]="sparkdatabricks"

#args[5] experiment instance number
args[5]="$2"
#args[6] directory for generated data raw files
args[6]="UNUSED"
#args[7] subdirectory within the jar that contains the create table files
args[7]="tables"
#args[8] suffix used for intermediate table text files
args[8]="_ext"
#args[9] prefix of external location for raw data tables (e.g. S3 bucket), null for none
args[9]="dbfs:/mnt/tpcdsbucket/$1GB"

#args[10] prefix of external location for created tables (e.g. S3 bucket), null for none
args[10]="dbfs:/mnt/tpcds-warehouses-test/$DirNameWarehouse"
#args[11] format for column-storage tables (PARQUET, DELTA)
args[11]="delta"
#args[12] whether to run queries to count the tuples generated (true/false)
args[12]="false"
#args[13] whether to use data partitioning for the tables (true/false)
args[13]="false"
#args[14] jar file
args[14]="/dbfs/mnt/tpcds-jars/targetsparkdatabricks/client-1.2-SNAPSHOT-jar-with-dependencies.jar"

#args[15] whether to generate statistics by analyzing tables (true/false)
args[15]="true"
#args[16] if argument above is true, whether to compute statistics for columns (true/false)
args[16]="true"
#args[17] queries dir within the jar
args[17]="QueriesSpark"
#args[18] subdirectory of work directory to store the results
args[18]="results"
#args[19] subdirectory of work directory to store the execution plans
args[19]="plans"

#args[20] save power test plans (boolean)
args[20]="true"
#args[21] save power test results (boolean)
args[21]="true"
#args[22] "all" or query file
args[22]="query2.sql"
#args[23] save tput test plans (boolean)
args[23]="true"
#args[24] save tput test results (boolean)
args[24]="true"
 
#args[25] number of streams
args[25]="$3"
#args[26] random seed
args[26]="1954"
#args[27] flags (111111 schema|load|analyze|zorder|power|tput)
args[27]="111010"

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

post_data_func()
{
  cat <<EOF
{ 
	"name":"$JOB_NAME",
	"new_cluster":{ 
		"spark_version":"${MajorVersion}.${MinorVersion}.x-scala2.11",
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
         "num_workers":$Nodes
      },
      "libraries":[ 
         { 
            "jar":"dbfs:/mnt/tpcds-jars/targetsparkdatabricks/client-1.2-SNAPSHOT-jar-with-dependencies.jar"
         }
      ],
      "email_notifications":{ 

      },
      "timeout_seconds":0,
      "spark_jar_task":{ 
         "jar_uri":"",
         "main_class_name":"org.bsc.dcc.vcv.RunBenchmarkSpark",
         "parameters":[ 
			$paramsStr
         ]
      },
      "max_concurrent_runs":1
}
EOF
}

#Get the state (RUNNING, TERMINATED) of a job run via its run_id using the Jobs REST API.
#$1 run_id
get_run_state() {
   declare jsonStr=$(curl -s -n \
   -H "Authorization: Bearer $DATABRICKS_TOKEN" \
   https://dbc-08fc9045-faef.cloud.databricks.com/api/2.0/jobs/runs/get?run_id=$1)

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
	https://dbc-08fc9045-faef.cloud.databricks.com/api/2.0/jobs/create)
	#Example output.
	#{"job_id":236}
   declare state=$(jq -j '.job_id'  <<<  "$jsonStr")
   echo $state
}

RUN_CREATE_AND_RUN_JOB=1
job_id=""
job_run_id=""

if [ "$RUN_CREATE_AND_RUN_JOB" -eq 1 ]; then
	echo "${blu}Creating job for benchmark execution.${end}"
	job_id=$(create_job)
	echo "${blu}Created job with id ${job_id} and starting its execution.${end}"
	jsonJobRun=$(databricks jobs run-now --job-id $job_id)
	job_run_id=$(jq -j '.run_id' <<< "$jsonJobRun")
fi

WAIT_FOR_TERMINATION=1

if [ "$WAIT_FOR_TERMINATION" -eq 1 ]; then
	echo "${blu}Waiting for the completion of run ${job_run_id}.${end}"
	wait_for_run_termination $job_run_id 120
	echo "${blu}Benchmark running job completed.${end}"
fi

#Delete the warehouse directory.

RUN_DELETE_WAREHOUSE=0

if [ "$RUN_DELETE_WAREHOUSE" -eq 1 ]; then
    echo "${blu}Deleting the warehouse directory ${DirNameWarehouse}.${end}"
    #Delete the bucket
    aws s3 rm --recursive s3://tpcds-warehouses-test/$DirNameWarehouse
    #exit 0
fi



