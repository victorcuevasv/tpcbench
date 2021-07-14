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
    echo "${yel}Usage: bash runclient_dbr_job_updates_multiple.sh <scale factor> <number of experiment instances> <number of streams>${end}"
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
#Note that /dbfs or dbfs: is not included, these are added later.
JarFile="/mnt/tpcds-jars/targetsparkdatabricks/client-1.2-SNAPSHOT-SHADED.jar"
#Script operation flags.
RUN_CREATE_JOB=1
RUN_RUN_JOB=1
JOB_NAME_PREFIX="Run TPC-DS Benchmark"

#Used by the script, do not edit.
JOB_NAME=""
args=()
paramsStr=""

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

#External metastore script:
#dbfs:/databricks/init_scripts/set_spark_embedded_metastore.sh

post_data_func()
{
  cat <<EOF
{ 
	"name":"$JOB_NAME",
	"new_cluster":{ 
		"spark_version":"custom:release__8.3.x-snapshot-photon-scala2.12__databricks-universe__head__095f3d4__815684c__jenkins__7a67413__format-2.lz4",
        "spark_conf":{
        	"spark.executor.memory":"8000m",
			"spark.memory.offHeap.enabled":"true",
			"spark.memory.offHeap.size":"36000m",
			"spark.databricks.photon.enabled":"true",
			"spark.databricks.photon.parquetWriter.enabled":"true",
			"spark.databricks.io.parquet.nativeReader.enabled":"false"
         },
         "aws_attributes":{ 
            "zone_id":"us-west-2b",
            "instance_profile_arn": "arn:aws:iam::384416317380:instance-profile/ShardS3Access_SSE-2051",
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
            "jar":"dbfs:$JarFile"
         }
      ],
      "email_notifications":{ 

      },
      "timeout_seconds":0,
      "spark_jar_task":{ 
         "jar_uri":"",
         "main_class_name":"org.bsc.dcc.vcv.RunBenchmarkSparkUpdateCLI",
         "parameters":[ 
			$paramsStr
         ]
      },
      "max_concurrent_runs":1
}
EOF
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

START=1
END=$2
for (( i=$START; i<=$END; i++ ))
do
	ExperimentName="tpcds-databricks-${MajorVersion}${MinorVersion}-$1gb-${Tag}"
	DirNameWarehouse="tpcds-databricks-${MajorVersion}${MinorVersion}-$1gb-${i}-${Tag}"
	DirNameResults="dbr${MajorVersion}${MinorVersion}"
	DatabaseName="tpcds_databricks_${MajorVersion}${MinorVersion}_$1gb_${i}_${Tag}"
	JOB_NAME="${JOB_NAME_PREFIX} ${Tag} ${i}"
	
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
	args[5]="--instance-number=${i}"
	# prefix of external location for raw data tables (e.g. S3 bucket), null for none
	args[6]="--ext-raw-data-location=dbfs:/mnt/tpcds-datasets/$1GB"
	# prefix of external location for created tables (e.g. S3 bucket), null for none
	args[7]="--ext-tables-location=dbfs:/mnt/tpcds-warehouses-test/$DirNameWarehouse"
	# format for column-storage tables (PARQUET, DELTA)
	args[8]="--table-format=parquet"
	# whether to use data partitioning for the tables (true/false)
	args[9]="--use-partitioning=true"
	
	# jar file
	args[10]="--jar-file=/dbfs$JarFile"
	# whether to generate statistics by analyzing tables (true/false)
	args[11]="--use-row-stats=true"
	# if argument above is true, whether to compute statistics for columns (true/false)
	args[12]="--use-column-stats=true"
	# "all" or query file
	args[13]="--all-or-query-file=all" 
	# number of streams
	args[14]="--number-of-streams=$3"
	# flags 111111100000111111100
	# schema      |load          |load denorm |load skip      |insupd data    |
	# delete data |load update   |analyze     |analyze denorm |analyze update |
	# zorder      |zorder update |read test 1 |insupd test    |read test 2    |
	# delete test |read test 3   |gdpr        |read test 4    |power          |
	# tput
	args[15]="--execution-flags=111111100000111111100"
	# "all" or create table file
	args[16]="--all-or-create-file=all"
	# count-queries
	args[17]="--count-queries=false"
	# all or denorm table file
	args[18]="--denorm-all-or-file=store_sales.sql"
	# skip data to be inserted later
	args[19]="--denorm-apply-skip=true"
	
	# all or query file for denorm analyze and z-order
	args[20]="--analyze-zorder-all-or-file=query2.sql"
	# customer surrogate key for the gdpr test (221580 for 1 TB, 1000 for 1 GB)
	args[21]="--gdpr-customer-sk=221580"
	# greater than threshold for the date-sk attribute (2452459 for last 10%, -1 to disable)
	args[22]="--datesk-gt-threshold=2452459"
	# use a filter attribute and value for denormalization
	args[23]="--denorm-with-filter=false"
	# add a distrubute by clause for denormalization with partition
	args[24]="--partition-with-distribute-by=true"
	
	printf "\n\n%s\n\n" "${mag}Creating the job for instance ${i}.${end}"
	
	paramsStr=$(json_string_list "${args[@]}")
	
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
	args=()
done

