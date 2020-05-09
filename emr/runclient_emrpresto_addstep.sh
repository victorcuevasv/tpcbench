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
    echo "${yel}Usage: bash runclient_emrpresto_addstep.sh <scale factor> <experiment instance number> <number of streams>${end}"
    exit 0
fi

ClusterId=""
DirNameWarehouse="tpcds-warehouse-prestoemr-529-$1gb-$2"
DirNameResults="1odwczxc3jftmhmvahdl7tz32dyyw0pen"
DatabaseName="tpcds_prestoemr_529_$1gb_$2_db"
Nodes="2"
JarFile="/mnt/tpcds-jars/targetemr/client-1.2-SNAPSHOT-SHADED.jar"

printf "\n\n%s\n\n" "${mag}Running the full TPC-DS benchmark.${end}"

args=()

#main work directory
args[0]="--main-work-dir=/data"
#schema (database) name
args[1]="--schema-name=$DatabaseName"
#results folder name (e.g. for Google Drive)
args[2]="--results-dir=$DirNameResults"
#experiment name (name of subfolder within the results folder)
args[3]="--experiment-name=prestoemr-529-${Nodes}nodes-$1gb-experimental"
#system name (system name used within the logs)
args[4]="--system-name=prestoemr"

#experiment instance number
args[5]="--instance-number=$2"
#prefix of external location for raw data tables (e.g. S3 bucket), null for none
args[6]="--ext-raw-data-location=s3://tpcds-datasets/$1GB"
#prefix of external location for created tables (e.g. S3 bucket), null for none
args[7]="--ext-tables-location=s3://tpcds-warehouses-test/$DirNameWarehouse"
#format for column-storage tables (PARQUET, DELTA)
args[8]="--table-format=orc"
#whether to use data partitioning for the tables (true/false)
args[9]="--use-partitioning=false"

#jar file
args[10]="--jar-file=$JarFile"
#whether to generate statistics by analyzing tables (true/false)
args[11]="--use-row-stats=true"
#if argument above is true, whether to compute statistics for columns (true/false)
args[12]="--use-column-stats=true"
#"all" or query file
args[13]="--all-or-query-file=all" 
#number of streams
args[14]="--number-of-streams=$3"

#flags (110000 schema|load|analyze|zorder|power|tput)
args[15]="--execution-flags=111011"
#whether to use bucketing for Hive and Presto
args[16]="--use-bucketing=false"
#hostname of the server
args[17]="--server-hostname=localhost"
#username for the connection
args[18]="--connection-username=hadoop"
#queries dir within the jar
args[19]="--queries-dir-in-jar=QueriesPresto"
#override the default system to use for data loading
#args[20]="--override-load-system=hive"
#override the default system to use for table statistics
#args[21]="--override-analysis-system=hive"
#all or create table file
#args[22]="--all-or-create-file=catalog_sales.sql"

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

#Options for action on failure: TERMINATE_CLUSTER, CANCEL_AND_WAIT, and CONTINUE
steps_func()
{
  cat <<EOF
[
   {
      "Args":[
		$paramsStr
      ],
      "Type":"CUSTOM_JAR",
      "ActionOnFailure":"CANCEL_AND_WAIT",
      "Jar":"$JarFile",
      "Properties":"",
      "Name":"Custom JAR"
   }
]
EOF
}

steps=$(jq -c . <<<  "$(steps_func)")

aws emr add-steps \
--cluster-id "$ClusterId" \
--steps "$steps"




