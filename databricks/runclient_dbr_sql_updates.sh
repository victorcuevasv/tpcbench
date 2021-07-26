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
    echo "${yel}Usage: bash runclient_dbr_sql_etl.sh <scale factor> <experiment instance number> <number of streams>${end}"
    exit 0
fi

#Execute the Java project with Maven on the client builder container running in the docker-compose setup. 

printf "\n\n%s\n\n" "${mag}Running the full TPC-DS benchmark.${end}"

DatabricksHost="dbc-08fc9045-faef.cloud.databricks.com"
#Run configuration.
Tag="$(date +%s)"
ExperimentName="tpcds-dbrsql-$1gb-${Tag}"
DirNameWarehouse="tpcds-dbrsql-$1gb-$2-${Tag}"
DirNameResults="dbrsql"
DatabaseName="tpcds_dbrsql_$1gb_$2_${Tag}"
JarFile="/mnt/tpcds-jars/target/client-1.2-SNAPSHOT-SHADED.jar"
NumCores=16
ClusterId="f18152ace277edb3"
DatabasePassword="" 

RUN_RUN_BENCHMARK=1
COPY_RESULTS_TO_S3=0

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
args[4]="--system-name=databrickssql"

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
args[10]="--jar-file=$JarFile"
# whether to generate statistics by analyzing tables (true/false)
args[11]="--use-row-stats=true"
# if argument above is true, whether to compute statistics for columns (true/false)
args[12]="--use-column-stats=true"
# "all" or query file
args[13]="--all-or-query-file=all" 
# "all" or create table file
args[14]="--all-or-create-file=all"

# number of streams
args[15]="--number-of-streams=$3"
# count-queries
args[16]="--count-queries=false"
# all or denorm table file
args[17]="--denorm-all-or-file=store_sales.sql"
# skip data to be inserted later
args[18]="--denorm-apply-skip=true"
# all or query file for denorm analyze and z-order
args[19]="--analyze-zorder-all-or-file=query2.sql"

# customer surrogate key for the gdpr test (221580 for 1 TB, 1000 for 1 GB)
args[20]="--gdpr-customer-sk=221580"
# greater than threshold for the date-sk attribute (2452459 for last 10%, -1 to disable)
args[21]="--datesk-gt-threshold=2452459"
# use a filter attribute and value for denormalization
args[22]="--denorm-with-filter=false"
# add a distrubute by clause for denormalization with partition
args[23]="--partition-with-distribute-by=true"
#hostname of the server
args[24]="--server-hostname=$DatabricksHost"

#username for the connection
args[25]="--connection-username=UNUSED"
#queries dir within the jar
args[26]="--queries-dir-in-jar=QueriesSpark"
#delimiter for the columns in the raw data (SOH, PIPE) default SOH
args[27]="--raw-column-delimiter=SOH"
#save power test plans
args[28]="--save-power-plans=true"
#identifier of the cluster to use to evaluate queries
args[29]="--cluster-id=${ClusterId}"
#database password

args[30]="--db-password=${DatabasePassword}"
#number of cores in the cluster to set the number of shuffle partitions
args[31]="--num-cores=${NumCores}"
#number of runs to perform for the power test (default 1)
args[32]="--power-test-runs=1"
#use multiple connections
args[33]="--multiple-connections=true"
# scale factor used to run the benchmark
args[34]="--scale-factor=$1"

# flags to activate and deactivate tests
# schema      |load          |analyze     |load denorm    |load skip      |
# insupd data |delete data   |load update |read test 1    |insupd test    |
# read test 2 |delete test   |read test 3 |gdpr           |read test 4    |
args[35]="--execution-flags=110111111111111"

paramsStr="${args[@]}"

if [ "$RUN_RUN_BENCHMARK" -eq 1 ]; then
	docker run --network="host" --rm --user $USER_ID:$GROUP_ID --name clientbuildercontainer -ti \
	--volume $DIR/../vols/data:/data \
	--volume $DIR/../client/project:/project \
	--volume $HOME/tpcdsbench/client/project/target:/mnt/tpcds-jars/target \
	--entrypoint mvn clientbuilder:dev \
	exec:java -Dexec.mainClass="org.bsc.dcc.vcv.tablestorage.RunTableStorageBenchmark" \
	-Dexec.args="$paramsStr" \
	-f /project/pom.xml
fi

if [ "$COPY_RESULTS_TO_S3" -eq 1 ]; then
	#aws s3 cp --recursive $DIR/../vols/data/$DirNameResults/ s3://tpcds-results-test/$DirNameResults/
	cp -r $DIR/../vols/data/$DirNameResults/* $HOME/tpcds-results-test/$DirNameResults/
fi


