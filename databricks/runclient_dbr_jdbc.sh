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

DatabricksHost="dbc-08fc9045-faef.cloud.databricks.com"
Nodes="16"
MajorVersion="7"
MinorVersion="2"
ScalaVersion="x-scala2.12"
#Run configuration.
Tag="$(date +%s)"
ExperimentName="tpcds-databricks-${MajorVersion}${MinorVersion}-$1gb-${Tag}"
DirNameWarehouse="tpcds-databricks-${MajorVersion}${MinorVersion}-$1gb-$2-${Tag}"
DirNameResults="dbr${MajorVersion}${MinorVersion}"
DatabaseName="tpcds_databricks_${MajorVersion}${MinorVersion}_$1gb_$2_${Tag}"
JarFile="/mnt/tpcds-jars/targetsparkjdbc/client-1.2-SNAPSHOT-SHADED.jar"

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
args[8]="--table-format=parquet"
#whether to use data partitioning for the tables (true/false)
args[9]="--use-partitioning=false"

#jar file
args[10]="--jar-file=$JarFile"
#whether to generate statistics by analyzing tables (true/false)
args[11]="--use-row-stats=true"
#if argument above is true, whether to compute statistics for columns (true/false)
args[12]="--use-column-stats=true"
#number of streams
args[14]="--number-of-streams=$3"
#hostname of the server
args[17]="--server-hostname=$DatabricksHost"

#username for the connection
args[18]="--connection-username=UNUSED"
#queries dir within the jar
args[19]="--queries-dir-in-jar=QueriesSpark"
#all or create table file
args[22]="--all-or-create-file=all"
#"all" or query file
args[13]="--all-or-query-file=all" 
#flags (110000 schema|load|analyze|zorder|power|tput)
args[15]="--execution-flags=111011"

function string_list() {
    declare array=("$@")
    declare list=""
    for w in "${array[@]}"
    do
        list+="$w "
    done
    #Remove the last space
    echo ${list%?}
}

paramsStr=$(string_list "${args[@]}")

docker run --network="host" --rm --user $USER_ID:$GROUP_ID --name clientbuildercontainer -ti \
--volume $HOME/data:/data \
--volume $DIR/../client/project:/project \
--entrypoint mvn clientbuilder:dev \
exec:java -Dexec.mainClass="org.bsc.dcc.vcv.RunBenchmark" \
-Dexec.args="$paramsStr" \
-f /project/pom.xml




