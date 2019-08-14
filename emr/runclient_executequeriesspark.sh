#!/bin/bash

#Variables for console output with colors.

red=$'\e[1;31m'
grn=$'\e[1;32m'
yel=$'\e[1;33m'
blu=$'\e[1;34m'
mag=$'\e[1;35m'
cyn=$'\e[1;36m'
end=$'\e[0m'

#Test query execution with Spark.

#Get the user name of the user executing this script.
USER_NAME=$(whoami)
#Get the user id of the user executing this script.
USER_ID=$(id -u)
#Get the user id of the user executing this script.
GROUP_ID=$(id -g)

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null && pwd )"

if [ $# -lt 3 ]; then
    echo "${yel}Usage: bash runclient_executequeriesspark.sh <scale factor> <experiment instance number> <all | query filename>${end}"      
    exit 0
fi

#args[0] main work directory
#args[1] schema (database) name
#args[2] results folder name (e.g. for Google Drive)
#args[3] experiment name (name of subfolder within the results folder)
#args[4] system name (system name used within the logs)

#args[5] test name (e.g. power)
#args[6] experiment instance number
#args[7] queries dir within the jar
#args[8] subdirectory of work directory to store the results
#args[9] subdirectory of work directory to store the execution plans

#args[10] save plans (boolean)
#args[11] save results (boolean)
#args[12] jar file
#args[13] "all" or query file

spark-submit --conf spark.eventLog.enabled=true  \
--packages org.apache.logging.log4j:log4j-api:2.11.2,org.apache.logging.log4j:log4j-core:2.11.2,\
org.apache.zookeeper:zookeeper:3.4.6 \
--class org.bsc.dcc.vcv.ExecuteQueriesSpark \
--master yarn --deploy-mode cluster \
hdfs://$(hostname)/project/targetspark/client-1.0-SNAPSHOT.jar \
/data tpcdsdb$1gb 13ox7IwkFEcRU61h2NXeAaSZMyTRzCby8 sparkemr2nodes sparkemr \
power $2 QueriesSpark results plans \
true true hdfs://$(hostname)/project/targetspark/client-1.0-SNAPSHOT.jar $3



