#!/bin/bash   

#Variables for console output with colors.

red=$'\e[1;31m'
grn=$'\e[1;32m'
yel=$'\e[1;33m'
blu=$'\e[1;34m'
mag=$'\e[1;35m'
cyn=$'\e[1;36m'
end=$'\e[0m'

#Get the user name of the user executing this script.
USER_NAME=$(whoami)
#Get the user id of the user executing this script.
USER_ID=$(id -u)
#Get the user id of the user executing this script.
GROUP_ID=$(id -g)

#$1 scale factor (positive integer)
#$2 experiment instance number (positive integer)
#$3 number of streams (positive integer)

if [ $# -lt 3 ]; then
    echo "${yel}Usage: bash runclient_fullbenchmarkspark_awscli.sh <scale factor> <experiment instance number> <number of streams>${end}"
    exit 0
fi

printf "\n\n%s\n\n" "${mag}Running the full TPC-DS benchmark.${end}"

DirNameWarehouse="tpcds-warehouse-sparkemr-529-$1gb-$2"
DirNameResults="1odwczxc3jftmhmvahdl7tz32dyyw0pen"
DatabaseName="tpcds_sparkemr_529_$1gb_$2_db"
Nodes="2"

args=()

#args[0] main work directory
args[0]="/data"
#args[1] schema (database) name
args[1]="$DatabaseName"
#args[2] results folder name (e.g. for Google Drive)
args[2]="$DirNameResults"
#args[3] experiment name (name of subfolder within the results folder)
args[3]="sparkemr-529-${Nodes}nodes-$1gb-experimental"
#args[4] system name (system name used within the logs)
args[4]="sparkemr"

#args[5] experiment instance number
args[5]="$2"
#args[6] directory for generated data raw files
args[6]="UNUSED"
#args[7] subdirectory within the jar that contains the create table files
args[7]="tables"
#args[8] suffix used for intermediate table text files
args[8]="_ext"
#args[9] prefix of external location for raw data tables (e.g. S3 bucket), null for none
args[9]="s3://tpcds-datasets/$1GB"

#args[10] prefix of external location for created tables (e.g. S3 bucket), null for none
args[10]="s3://tpcds-warehouses-test/$DirNameWarehouse"
#args[11] format for column-storage tables (PARQUET, DELTA)
args[11]="parquet"
#args[12] whether to run queries to count the tuples generated (true/false)
args[12]="false"
#args[13] whether to use data partitioning for the tables (true/false)
args[13]="true"
#args[14] jar file
args[14]="/mnt/tpcds-jars/targetsparkdatabricks/client-1.2-SNAPSHOT-jar-with-dependencies.jar"

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

ec2-attributes_func()
{
  cat <<EOF
{
   "KeyName":"testalojakeypair",
   "InstanceProfile":"EMR_EC2_DefaultRole",
   "SubnetId":"subnet-01033078",
   "EmrManagedSlaveSecurityGroup":"sg-0d6c7aa7f3a231e50",
   "EmrManagedMasterSecurityGroup":"sg-0cefde07cc1a0a36e"
}
EOF
}


steps_func()
{
  cat <<EOF
[
   {
      "Args":[
         "spark-submit",
         "--deploy-mode",
         "client",
         "--conf",
         "spark.eventLog.enabled=true",
         "--class",
         "org.bsc.dcc.vcv.RunBenchmarkSpark",
         "${args[14]}",
         $paramsStr
      ],
      "Type":"CUSTOM_JAR",
      "ActionOnFailure":"TERMINATE_CLUSTER",
      "Jar":"command-runner.jar",
      "Properties":"",
      "Name":"Spark application"
   }
]
EOF
}

instance-groups_func()
{
  cat <<EOF
[
   {
      "InstanceCount":$Nodes,
      "InstanceGroupType":"CORE",
      "InstanceType":"i3.2xlarge",
      "Name":"Core - $Nodes"
   },
   {
      "InstanceCount":1,
      "InstanceGroupType":"MASTER",
      "InstanceType":"i3.2xlarge",
      "Name":"Master - 1"
   }
]
EOF
}


configurations_func()
{
  cat <<EOF
[
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
EOF
}


bootstrap-actions_func()
{
  cat <<EOF
[
   {
      "Path":"s3://bsc-bootstrap/s3fs/emr_init.sh",
      "Args":[
         "hadoop",
         "tpcds-jars,tpcds-results-test"
      ],
      "Name":"Custom action"
   }
]
EOF
}

#Get the state (RUNNING, TERMINATED) of a job run via its run_id using the Jobs REST API.
#$1 run_id
get_run_state() {
   declare jsonStr=$(aws emr describe-cluster --cluster-id $1)
   declare state=$(jq -j '.Cluster.Status.State'  <<<  "$jsonStr")
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

ec2Attributes=$(jq -c . <<<  "$(ec2-attributes_func)")
steps=$(jq -c . <<<  "$(steps_func)")
instanceGroups=$(jq -c . <<<  "$(instance-groups_func)")
configurations=$(jq -c . <<<  "$(configurations_func)")
bootstrapActions=$(jq -c . <<<  "$(bootstrap-actions_func)")

#Create the cluster and run the benchmark.

RUN_CREATE_CLUSTER=0
jsonCluster=""
cluster_id=""

if [ "$RUN_CREATE_CLUSTER" -eq 1 ]; then
    jsonCluster=$(aws emr create-cluster \
	--termination-protected \
	--applications Name=Hadoop Name=Hive Name=Spark \
	--ec2-attributes "$ec2Attributes" \
	--release-label emr-5.29.0 \
	--log-uri 's3n://bsc-emr-logs/' \
	--steps "$steps" \
	--instance-groups "$instanceGroups" \
	--configurations "$configurations" \
	--auto-terminate \
	--auto-scaling-role EMR_AutoScaling_DefaultRole \
	--bootstrap-actions "$bootstrapActions" \
	--ebs-root-volume-size 10 \
	--service-role EMR_DefaultRole \
	--enable-debugging \
	--name 'BSC-test' \
	--scale-down-behavior TERMINATE_AT_TASK_COMPLETION \
	--region us-west-2)
    #exit 0
    #Example output.
	#{
    #	"ClusterId": "j-I7ZKG7UYDEDA",
    #	"ClusterArn": "arn:aws:elasticmapreduce:us-west-2:384416317380:cluster/j-I7ZKG7UYDEDA"
	#}
	cluster_id=$(jq -j '.ClusterId'  <<<  "$jsonCluster")
	echo "${blu}Created cluster with id ${cluster_id}.${end}"
fi

WAIT_FOR_TERMINATION=1

if [ "$WAIT_FOR_TERMINATION" -eq 1 ]; then
	echo "${blu}Waiting for the completion of cluster ${cluster_id}.${end}"
	wait_for_run_termination $cluster_id 120
	echo "${blu}Execution complete.${end}"
fi

#Delete the warehouse directory.

RUN_DELETE_WAREHOUSE=0

if [ "$RUN_DELETE_WAREHOUSE" -eq 1 ]; then
    echo "${blu}Deleting the warehouse directory ${DirNameWarehouse}.${end}"
    #Delete the bucket
    aws s3 rb s3://tpcds-warehouses-test/$DirNameWarehouse --force
    #exit 0
fi


