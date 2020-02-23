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

#Create and populate the database from the .dat files. The scale factor is passed as an argument
#and used to identify the folder that holds the data.
#$1 scale factor (positive integer)
#$2 experiment instance number (positive integer)
#$3 number of streams (positive integer)
#$4 number of worker threads (positive integer)

if [ $# -lt 4 ]; then
    echo "${yel}Usage: bash runclient_fullbenchmark_conc_limited.sh <scale factor> <experiment instance number> <number of streams> <number of workers>${end}"
    exit 0
fi

#Execute the Java project with Maven on the client builder container running in the docker-compose setup. 

printf "\n\n%s\n\n" "${mag}Running the full TPC-DS benchmark.${end}"

args=()

#args[0] main work directory
args[0]="/data"
#args[1] schema (database) name
args[1]="tpcdsdb$1gb"
#args[2] results folder name (e.g. for Google Drive)
args[2]="19aoujv0ull8kx87l4by700xikfythorv"
#args[3] experiment name (name of subfolder within the results folder)
args[3]="snowflake-large-$1gb-tput-$3streams-$4workers"
#args[4] system name (system name used within the logs)
args[4]="snowflake"

#args[5] experiment instance number
args[5]="$2"
#args[6] directory for generated data raw files
args[6]="UNUSED"
#args[7] subdirectory within the jar that contains the create table files
args[7]="tables"
#args[8] suffix used for intermediate table text files
args[8]="_ext"
#args[9] prefix of external location for raw data tables (e.g. S3 bucket), null for none
args[9]="TPCDSDB$1GB_S3_STAGE/$1GB"

#args[10] prefix of external location for created tables (e.g. S3 bucket), null for none
args[10]="null"
#args[11] format for column-storage tables (PARQUET, DELTA)
args[11]="UNUSED"
#args[12] whether to run queries to count the tuples generated (true/false)
args[12]="false"
#args[13] whether to use data partitioning for the tables (true/false)
args[13]="false"
#args[14] hostname of the server
args[14]="zua56993.snowflakecomputing.com"

#args[15] username for the connection
args[15]="bsctest"
#args[16] jar file
args[16]="/project/target/client-1.0-SNAPSHOT.jar"
#args[17] whether to generate statistics by analyzing tables (true/false)
args[17]="false"
#args[18] if argument above is true, whether to compute statistics for columns (true/false)
args[18]="UNUSED"
#args[19] queries dir within the jar
args[19]="QueriesSnowflake"

#args[20] subdirectory of work directory to store the results
args[20]="results"
#args[21] subdirectory of work directory to store the execution plans
args[21]="plans"
#args[22] save power test plans (boolean)
args[22]="false"
#args[23] save power test results (boolean)
args[23]="true"
#args[24] "all" or query file
args[24]="all"
 
#args[25] save tput test plans (boolean)
args[25]="false"
#args[26] save tput test results (boolean)
args[26]="true"
#args[27] number of streams
args[27]="$3"
#args[28] random seed
args[28]="1954"
#args[29] number of workers
args[29]="$4"

#args[30] use multiple connections (true|false)
args[30]="true"
#args[31] flags (111111 schema|load|analyze|zorder|power|tput)
args[31]="000001"

docker run --network="host" --rm --user $USER_ID:$GROUP_ID --name clientbuildercontainer -ti \
--volume /data:/data \
--volume $DIR/../client/project:/project \
--volume $DIR/../vols/hive:/temporal \
--entrypoint mvn clientbuilder:dev \
exec:java -Dexec.mainClass="org.bsc.dcc.vcv.RunBenchmarkLimit" \
-Dexec.args="${args[*]}" \
-f /project/pom.xml


