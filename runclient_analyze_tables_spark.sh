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

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null && pwd )"

#Create and populate the database from the .dat files. The scale factor is passed as an argument
#and used to identify the folder that holds the data.
#$1 scale factor (positive integer)
#$2 experiment instance number (positive integer)

if [ $# -lt 2 ]; then
    echo "${yel}Usage: bash runclient_analyze_tables_spark.sh <scale factor> <experiment instance number>${end}"
    exit 0
fi

#Execute the Java project with Maven on the client builder container running in the docker-compose setup. 

printf "\n\n%s\n\n" "${mag}Creating and populating the database.${end}"

#args[0] main work directory
#args[1] schema (database) name
#args[2] results folder name (e.g. for Google Drive)
#args[3] experiment name (name of subfolder within the results folder)
#args[4] system name (system name used within the logs)

#args[5] test name (i.e. load)
#args[6] experiment instance number
#args[7] compute statistics for columns (true/false)

docker exec --user $USER_ID:$GROUP_ID -ti  namenodecontainer  /bin/bash -c \
"/opt/spark-2.4.0-bin-hadoop2.7/bin/spark-submit --conf spark.eventLog.enabled=true  \
--packages org.apache.logging.log4j:log4j-api:2.11.2,org.apache.logging.log4j:log4j-core:2.11.2,\
org.apache.zookeeper:zookeeper:3.4.6 \
--conf spark.local.dir=/home/$USER_NAME/tmp \
--conf spark.eventLog.dir=/home/$USER_NAME/tmp \
--class org.bsc.dcc.vcv.AnalyzeTablesSpark \
--master spark://namenodecontainer:7077 --deploy-mode client \
/project/targetspark/client-1.0-SNAPSHOT.jar \
/data tpcdsdb$1gb 13ox7IwkFEcRU61h2NXeAaSZMyTRzCby8 sparksinglenode spark \
analyze $2 true"


