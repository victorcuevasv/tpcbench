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

if [ $# -lt 2 ]; then
    echo "${yel}Usage: bash runclient_executequeriesspark.sh <scale factor> <all | query filename>${end}"
    exit 0
fi

spark-submit --conf spark.eventLog.enabled=true  \
--packages org.apache.logging.log4j:log4j-api:2.11.2,org.apache.logging.log4j:log4j-core:2.11.2,\
org.apache.zookeeper:zookeeper:3.4.6 \
--class org.bsc.dcc.vcv.ExecuteQueriesSpark \
--master yarn --deploy-mode cluster \
hdfs://$(hostname)/project/targetspark/client-1.0-SNAPSHOT.jar \
/data results plans hdfs://$(hostname)/project/targetspark/client-1.0-SNAPSHOT.jar spark true true tpcdsdb$1gb $2 



