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
    echo "${yel}Usage: bash runclient_fullbenchmark_jdbc.sh <scale factor> <experiment instance number> <number of streams>${end}"
    exit 0
fi

#Execute the Java project with Maven on the client builder container running in the docker-compose setup. 

printf "\n\n%s\n\n" "${mag}Running the full TPC-DS benchmark.${end}"

Host="bsc-redshift-cluster-test.cha8h2ua3ess.us-west-2.redshift.amazonaws.com"
#Run configuration.
Tag="$(date +%s)"
ExperimentName="tpcds-redshift-$1gb-${Tag}"
DirNameWarehouse="tpcds-redshift-$1gb-$2-${Tag}"
DirNameResults="redshift"
DatabaseName="tpcds_redshift_$1gb_$2_${Tag}"
JarFile="/mnt/tpcds-jars/target/client-1.2-SNAPSHOT-SHADED.jar"
DatabaseUsername="awsuser"

RUN_RUN_BENCHMARK=1
COPY_RESULTS_TO_S3=0

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
args[4]="--system-name=redshift"

#experiment instance number
args[5]="--instance-number=$2"
#prefix of external location for raw data tables (e.g. S3 bucket), null for none
args[6]="--ext-raw-data-location=s3://tpcds-datasets/$1GB"
#prefix of external location for created tables (e.g. S3 bucket), null for none
args[7]="--ext-tables-location=s3://tpcds-warehouses-test/$DirNameWarehouse"
#format for column-storage tables (PARQUET, DELTA)
args[8]="--table-format=UNUSED"
#whether to use data partitioning for the tables (true/false)
args[9]="--use-partitioning=false"

#jar file
args[10]="--jar-file=$JarFile"
#whether to generate statistics by analyzing tables (true/false)
args[11]="--use-row-stats=true"
#if argument above is true, whether to compute statistics for columns (true/false)
args[12]="--use-column-stats=true"
#number of streams
args[13]="--number-of-streams=$3"
#hostname of the server
args[14]="--server-hostname=$Host"

#username for the connection
args[15]="--connection-username=${DatabaseUsername}"
#queries dir within the jar
args[16]="--queries-dir-in-jar=QueriesRedshift"
#all or create table file
args[17]="--all-or-create-file=all"
#"all" or query file
args[18]="--all-or-query-file=all" 
#delimiter for the columns in the raw data (SOH, PIPE) default SOH
args[19]="--raw-column-delimiter=SOH"

#use distribute by keys in redshift and synapse
args[20]="--use-distribute-keys=false"
#number of runs to perform for the power test (default 1)
args[21]="--power-test-runs=1"
#flags (110000 schema|load|analyze|zorder|power|tput)
args[22]="--execution-flags=110010"

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

if [ "$COPY_RESULTS_TO_S3" -eq 1 ]; then
	#aws s3 cp --recursive $DIR/../vols/data/$DirNameResults/ s3://tpcds-results-test/$DirNameResults/
	cp -r $DIR/../vols/data/$DirNameResults/* $HOME/tpcds-results-test/$DirNameResults/
fi



