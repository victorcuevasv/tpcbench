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

if [ $# -lt 3 ]; then
    echo "${yel}Usage: bash runclient_fullbenchmark.sh <scale factor> <experiment instance number> <number of streams>${end}"
    exit 0
fi

#Execute the Java project with Maven on the client builder container running in the docker-compose setup. 

printf "\n\n%s\n\n" "${mag}Running the full TPC-DS benchmark.${end}"

#args[0] main work directory
#args[1] schema (database) name
#args[2] results folder name (e.g. for Google Drive)
#args[3] experiment name (name of subfolder within the results folder)
#args[4] system name (system name used within the logs)
 
#args[5] experiment instance number
#args[6] directory for generated data raw files
#args[7] subdirectory within the jar that contains the create table files
#args[8] suffix used for intermediate table text files
#args[9] prefix of external location for raw data tables (e.g. S3 bucket), null for none
 
#args[10] prefix of external location for created tables (e.g. S3 bucket), null for none
#args[11] format for column-storage tables (PARQUET, DELTA)
#args[12] whether to run queries to count the tuples generated (true/false)
#args[13] hostname of the server
#args[14] username for the connection
 
#args[15] queries dir within the jar
#args[16] subdirectory of work directory to store the results
#args[17] subdirectory of work directory to store the execution plans
#args[18] save power test plans (boolean)
#args[19] save power test results (boolean)
 
#args[20] "all" or query file
#args[21] save tput test plans (boolean)
#args[22] save tput test results (boolean)
#args[23] number of streams
#args[24] random seed
 
#args[25] use multiple connections (true|false)

docker run --network="host" --rm --user $USER_ID:$GROUP_ID --name clientbuildercontainer -ti \
--volume $DIR/../vols/data:/data \
--volume $DIR/../client/project:/project \
--entrypoint mvn clientbuilder:dev \
exec:java -Dexec.mainClass="org.bsc.dcc.vcv.CreateDatabase" \
-Dexec.args="/data tpcdsdb$1gb 13ox7IwkFEcRU61h2NXeAaSZMyTRzCby8 prestoemr2nodes prestoemr \
$2 UNUSED tables _ext s3://tpcds-datasets/$1GB \
s3://tpcds-warehouse-emr-presto-$1gb orc false $(hostname) $(whoami) \
QueriesPresto results plans true true \
all true true $3 1954 \
false" \
-f /project/pom.xml





  


