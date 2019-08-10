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

if [ $# -lt 3 ]; then
    echo "${yel}Usage: bash runclient_executequeriesspark.sh <scale factor> <experiment instance number> <all | query filename>${end}"
    exit 0
fi

docker exec --user $USER_ID:$GROUP_ID -ti  namenodecontainer  /bin/bash -c \
"/opt/spark-2.4.0-bin-hadoop2.7/bin/spark-submit --conf spark.eventLog.enabled=true  \
--packages org.apache.logging.log4j:log4j-api:2.11.2,org.apache.logging.log4j:log4j-core:2.11.2,\
org.apache.zookeeper:zookeeper:3.4.6 \
--conf spark.local.dir=/home/$USER_NAME/tmp \
--conf spark.eventLog.dir=/home/$USER_NAME/tmp \
--class org.bsc.dcc.vcv.ExecuteQueriesSpark \
--master spark://namenodecontainer:7077 --deploy-mode client \
/project/targetspark/client-1.0-SNAPSHOT.jar \
/data results plans /project/targetspark/client-1.0-SNAPSHOT.jar spark true true tpcdsdb$1gb power QueriesSpark \
/data/13ox7IwkFEcRU61h2NXeAaSZMyTRzCby8 spark $2 $3"                
	
