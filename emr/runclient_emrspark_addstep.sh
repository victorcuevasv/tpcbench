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
    echo "${yel}Usage: bash runclient_emrspark_addstep.sh <scale factor> <experiment instance number> <number of streams>${end}"
    exit 0
fi

printf "\n\n%s\n\n" "${mag}Running the full TPC-DS benchmark.${end}"

ClusterId=""
Tag=$(date +%s)
ExperimentName="sparkemr-600-${Nodes}nodes-$1gb-$Tag"
DirNameWarehouse="tpcds-warehouse-sparkemr-600-$1gb-$2-$Tag"
DirNameResults="1odwczxc3jftmhmvahdl7tz32dyyw0pen"
DatabaseName="tpcds_sparkemr_600_$1gb_$2_db_$Tag"
JarFile="/mnt/tpcds-jars/targetsparkdatabricks/client-1.2-SNAPSHOT-SHADED.jar"
AutoTerminate="true"

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
args[4]="--system-name=sparkemr"

# experiment instance number
args[5]="--instance-number=$2"
# prefix of external location for raw data tables (e.g. S3 bucket), null for none
args[6]="--ext-raw-data-location=s3://tpcds-datasets/$1GB"
# prefix of external location for created tables (e.g. S3 bucket), null for none
args[7]="--ext-tables-location=s3://tpcds-warehouses-test/$DirNameWarehouse"
# format for column-storage tables (PARQUET, DELTA)
args[8]="--table-format=parquet"
# whether to use data partitioning for the tables (true/false)
args[9]="--use-partitioning=false"

# "all" or create table file
args[10]="--all-or-create-file=all"
# jar file
args[11]="--jar-file=$JarFile"
# whether to generate statistics by analyzing tables (true/false)
args[12]="--use-row-stats=true"
# if argument above is true, whether to compute statistics for columns (true/false)
args[13]="--use-column-stats=true"
# "all" or query file
args[14]="--all-or-query-file=query2.sql"
 
# number of streams
args[15]="--number-of-streams=$3"
# flags (111111 schema|load|analyze|zorder|power|tput)
args[16]="--execution-flags=111011"

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
         "spark-submit",
         "--deploy-mode",
         "client",
         "--conf",
         "spark.eventLog.enabled=true",
         "--class",
         "org.bsc.dcc.vcv.RunBenchmarkSparkCLI",
         "$JarFile",
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

steps=$(jq -c . <<<  "$(steps_func)")

aws emr add-steps \
--cluster-id "$ClusterId" \
--steps "$steps"


