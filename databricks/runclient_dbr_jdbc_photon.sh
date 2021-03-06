#!/bin/bash   

#Variables for console output with colors.

red=$'\e[1;31m'
grn=$'\e[1;32m'
yel=$'\e[1;33m'
blu=$'\e[1;34m'
mag=$'\e[1;35m'
cyn=$'\e[1;36m'
end=$'\e[0m'

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null && pwd )"

#Get the user id of the user executing this script.
USER_ID=$(id -u)
#Get the user id of the user executing this script.
GROUP_ID=$(id -g)

#$1 scale factor (positive integer)
#$2 experiment instance number (positive integer)
#$3 number of streams (positive integer)

if [ $# -lt 3 ]; then
    echo "${yel}Usage: bash runclient_dbr_jdbc_photon.sh <scale factor> <experiment instance number> <number of streams>${end}"
    exit 0
fi

#Execute the Java project with Maven on the client builder container running in the docker-compose setup. 

printf "\n\n%s\n\n" "${mag}Running the full TPC-DS benchmark.${end}"

DatabricksHost="dbc-08fc9045-faef.cloud.databricks.com"
Nodes="16"
MajorVersion="8"
MinorVersion="3p"
ScalaVersion="x-scala2.12"
#Run configuration.
Tag="$(date +%s)"
ExperimentName="tpcds-dbrjdbc-${MajorVersion}${MinorVersion}-$1gb-${Tag}"
DirNameWarehouse="tpcds-dbrjdbc-${MajorVersion}${MinorVersion}-$1gb-$2-${Tag}"
DirNameResults="dbr${MajorVersion}${MinorVersion}jdbc"
DatabaseName="tpcds_dbrjdbc_${MajorVersion}${MinorVersion}_$1gb_$2_${Tag}"
DatabasePassword=""
JarFile="/mnt/tpcds-jars/target/client-1.2-SNAPSHOT-SHADED.jar"

CLUSTER_NAME="TPC-DS_${Tag}_$2"
RUN_CREATE_CLUSTER=1
cluster_id=""
RUN_RUN_BENCHMARK=1
RUN_TERMINATE_CLUSTER=1
COPY_RESULTS_TO_S3=1

args=()

#main work directory
args[0]="--main-work-dir=/data"
#schema (database) name
args[1]="--schema-name=$DatabaseName"
#results folder name (e.g. for Google Drive)
args[2]="--results-dir=$DirNameResults"
#experiment name (name of subfolder within the results folder)
args[3]="--experiment-name=$ExperimentName"
#system name (system name used within the logs)
args[4]="--system-name=sparkdatabricksjdbc"

#experiment instance number
args[5]="--instance-number=$2"
#prefix of external location for raw data tables (e.g. S3 bucket), null for none
args[6]="--ext-raw-data-location=s3://tpcds-datasets/$1GB"
#prefix of external location for created tables (e.g. S3 bucket), null for none
args[7]="--ext-tables-location=s3://tpcds-warehouses-test/$DirNameWarehouse"
#format for column-storage tables (PARQUET, DELTA)
args[8]="--table-format=delta"
#whether to use data partitioning for the tables (true/false)
args[9]="--use-partitioning=true"

#jar file
args[10]="--jar-file=$JarFile"
#whether to generate statistics by analyzing tables (true/false)
args[11]="--use-row-stats=true"
#if argument above is true, whether to compute statistics for columns (true/false)
args[12]="--use-column-stats=true"
#number of streams
args[13]="--number-of-streams=$3"
#hostname of the server
args[14]="--server-hostname=$DatabricksHost"

#username for the connection
args[15]="--connection-username=UNUSED"
#queries dir within the jar
args[16]="--queries-dir-in-jar=QueriesSpark"
#all or create table file
args[17]="--all-or-create-file=all"
#"all" or query file
args[18]="--all-or-query-file=all"
#delimiter for the columns in the raw data (SOH, PIPE) default SOH
args[19]="--raw-column-delimiter=SOH"

#number of runs to perform for the power test (default 1)
args[20]="--power-test-runs=1"
#database password
args[21]="--db-password=${DatabasePassword}"
#identifier of the cluster to use to evaluate queries (assigned below, after cluster creation)
args[22]="--cluster-id="
#use multiple connections
args[23]="--multiple-connections=true"
#flags (110000 schema|load|analyze|zorder|power|tput)
args[24]="--execution-flags=111011"

#Wait until the cluster is in a given state by polling using the Databricks CLI.
#$1 cluster_id
#$2 state waiting for
#$3 pause in seconds for polling
wait_for_state() {
   declare state=$(get_cluster_state $1)
   while [ $state != "$2"  ] ;
   do
      sleep $3
      state=$(get_cluster_state $1)
   done
}

#Get the state (PENDING, RUNNING, TERMINATED) of a cluster via its cluster_id using the Databricks CLI.
#$1 run_id
get_cluster_state() {
	declare jsonStr=""
	jsonStr=$(databricks clusters get --cluster-id $1)
	declare state=$(jq -j '.state'  <<<  "$jsonStr")
	echo $state
}


post_data_func()
{
  cat <<EOF
{
    "num_workers": $Nodes,
    "cluster_name":"$CLUSTER_NAME",
    "spark_version": "custom:release__8.3.x-snapshot-photon-scala2.12__databricks-universe__head__83b02b5__e760fb6__jenkins__6aae6d7__format-2.lz4",
    "spark_conf":{
            "spark.executor.memory":"8000m",
        	"spark.memory.offHeap.enabled":"true",
        	"spark.memory.offHeap.size":"36000m",
        	"spark.databricks.photon.enabled":"true",
        	"spark.databricks.photon.parquetWriter.enabled":"true",
            "spark.databricks.io.parquet.nativeReader.enabled":"false",
            "spark.databricks.delta.optimizeWrite.enabled":"true"
    },
    "aws_attributes":{ 
         "zone_id":"us-west-2b",
         "instance_profile_arn": "arn:aws:iam::384416317380:instance-profile/ShardS3Access_SSE-2051",
         "availability":"ON_DEMAND"
    },
    "node_type_id": "i3.2xlarge",
    "driver_node_type_id": "i3.8xlarge",
    "ssh_public_keys": [
        "ssh-rsa AAAAB3NzaC1yc2EAAAADAQABAAABAQDjxM9WRCj8vI0aDAVhVi3DkibIaChqwjshgcPvXcth5l1cWxBZLkDdL1CCLSAbUGGL+HX79FL7L46wBCOHTJ2hhd7tjxxDG7IeH/G2Q1wPsTMt7Vpswc8Ijp6BKzbqAJS9HJAq9VPjh0x39gPd2x4vHiRpudKA+RFvTQ1jWRz0nTxI/eteZWB03jrPQxbZFo5v/29VDTwRlDBraC5q3hblfXAUk8cWQkhlFz2XagPNzsigVsY/zJvIJ/zW5ZpPI5El2VK3CEGjqbg5qt7QnIiRydly3N2eWHDIwZM3nfAMYdWig+65U8LOy9NC8J6dk8v/ZlstoOLNNm5+LSkmj9b7 pristine@al-1001"
    ],
    "custom_tags": {},
    "spark_env_vars": {},
    "enable_elastic_disk": false,
    "init_scripts": []
}
EOF
}


if [ "$RUN_CREATE_CLUSTER" -eq 1 ]; then
	echo "${blu}Creating cluster for benchmark execution.${end}"
	#Must add the quotes to post_data_func to avoid error.
	jsonClusterCreate=$(databricks clusters create --json "$(post_data_func)")
	cluster_id=$(jq -j '.cluster_id'  <<<  "$jsonClusterCreate")
	echo "${blu}Launched creation of cluster with id ${cluster_id}.${end}"
	echo "${blu}Waiting for the initialization of cluster with id ${cluster_id}.${end}"
	wait_for_state $cluster_id "RUNNING" 60
	echo "${blu}Cluster running.${end}"
fi

#add the cluster id to the parameters
args[22]="--cluster-id=$cluster_id"
paramsStr="${args[@]}"

if [ "$RUN_RUN_BENCHMARK" -eq 1 ]; then
	docker run --network="host" --rm --user $USER_ID:$GROUP_ID --name clientbuildercontainer -ti \
	--volume $DIR/../vols/data:/data \
	--volume $DIR/../client/project:/project \
	--volume $HOME/tpcdsbench/client/project/target:/mnt/tpcds-jars/target \
	--entrypoint mvn clientbuilder:dev \
	exec:java -Dexec.mainClass="org.bsc.dcc.vcv.RunBenchmarkCLI" \
	-Dexec.args="$paramsStr" \
	-f /project/pom.xml
fi

if [ "$RUN_TERMINATE_CLUSTER" -eq 1 ]; then
	echo "${blu}Terminating cluster used for benchmark execution.${end}"
	#Must add the quotes to post_data_func to avoid error.
	databricks clusters delete --cluster-id $cluster_id
	echo "${blu}Launched termination of cluster with id ${cluster_id}.${end}"
	echo "${blu}Waiting for the termination of cluster with id ${cluster_id}.${end}"
	wait_for_state $cluster_id "TERMINATED" 20
	echo "${blu}Cluster terminated.${end}"
fi


if [ "$COPY_RESULTS_TO_S3" -eq 1 ]; then
	cp -r $DIR/../vols/data/$DirNameResults/* $HOME/tpcds-results-test/$DirNameResults/
fi

