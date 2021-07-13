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
    echo "${yel}Usage: bash runclient_dbr_job_updates_multiple_notebook.sh <scale factor> <number of experiment instances> <number of streams>${end}"
    exit 0
fi

printf "\n\n%s\n\n" "${mag}Running the TPC-DS benchmark.${end}"

#Cluster configuration.
DatabricksHost="dbc-08fc9045-faef.cloud.databricks.com"
Nodes="16"
MajorVersion="8"
MinorVersion="3"
ScalaVersion="x-scala2.12"
#Run configuration.
Tag="$(date +%s)"
#Script operation flags.
RUN_CREATE_JOB=1
RUN_RUN_JOB=1
JOB_NAME_PREFIX="Run TPC-DS Benchmark"

#Used by the script, do not edit.
JOB_NAME=""

#External metastore script:
#dbfs:/databricks/init_scripts/set_spark_embedded_metastore.sh

post_data_func()
{
  cat <<EOF
{ 
	"name":"$JOB_NAME",
	"new_cluster":{ 
		"spark_version":"${MajorVersion}.${MinorVersion}.${ScalaVersion}",
        "spark_conf":{
            "spark.databricks.delta.optimizeWrite.enabled":"true"
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
      "email_notifications":{ 

      },
      "timeout_seconds":0,
      "notebook_task": {
            "notebook_path": "/Engineering/Benchmarking/TPC/TPC-notebook-runner",
            "base_parameters": {
                "scaleFactor": "$1",
                "instance": "${instance}",
                "tag": "${Tag}",
                "majorVersion": "${MajorVersion}",
                "minorVersion": "${MinorVersion}"
            }
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
	printf "\n\n%s\n\n" "${mag}Creating the job for instance ${i}.${end}"
	JOB_NAME="${JOB_NAME_PREFIX} ${Tag} ${i}"
	instance="${i}"
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

