#!/bin/bash

#Test query execution with Spark.
#$1 Optional argument denoting a single query to execute (e.g. query3.sql).

#Get the user name of the user executing this script.
USER_NAME=$(whoami)
#Get the user id of the user executing this script.
USER_ID=$(id -u)
#Get the user id of the user executing this script.
GROUP_ID=$(id -g)

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null && pwd )"

#spark-submit --conf spark.eventLog.enabled=true  \
#--packages org.apache.logging.log4j:log4j-api:2.8.2,org.apache.logging.log4j:log4j-core:2.8.2,\
#org.apache.zookeeper:zookeeper:3.4.6 \
#--conf spark.local.dir=/home/$USER_NAME/tmp \
#--conf spark.eventLog.dir=/home/$USER_NAME/tmp \
#--class org.bsc.dcc.vcv.ExecuteQueriesSpark \
#--master yarn --deploy-mode client \
#$DIR/../client/project/targetspark/client-1.0-SNAPSHOT.jar \
#/data results plans $DIR/../client/project/targetspark/client-1.0-SNAPSHOT.jar spark true true $1                
	

spark-submit --conf spark.eventLog.enabled=true  \
--packages org.apache.logging.log4j:log4j-api:2.8.2,org.apache.logging.log4j:log4j-core:2.8.2,\
org.apache.zookeeper:zookeeper:3.4.6 \
--class org.bsc.dcc.vcv.ExecuteQueriesSpark \
--master yarn --deploy-mode client \
$DIR/../client/project/targetspark/client-1.0-SNAPSHOT.jar \
/data results plans $DIR/../client/project/targetspark/client-1.0-SNAPSHOT.jar spark true true $1 



