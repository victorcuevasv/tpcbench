#!/bin/bash

#Test query execution with Spark.

#Get the user name of the user executing this script.
USER_NAME=$(whoami)
#Get the user id of the user executing this script.
USER_ID=$(id -u)
#Get the user id of the user executing this script.
GROUP_ID=$(id -g)

#$1 Required argument denoting the number of streams. 

if [ $# -lt 1 ]; then
    echo "Usage bash runclient_executequeriesconcurrentspark.sh <number of streams>."
    exit 0
fi

docker exec --user $USER_ID:$GROUP_ID -ti  namenodecontainer  /bin/bash -c \
"/opt/spark-2.4.0-bin-hadoop2.7/bin/spark-submit --conf spark.eventLog.enabled=true  \
--packages org.apache.logging.log4j:log4j-api:2.8.2,org.apache.logging.log4j:log4j-core:2.8.2,\
org.apache.zookeeper:zookeeper:3.4.6 \
--conf spark.local.dir=/home/$USER_NAME/tmp \
--conf spark.eventLog.dir=/home/$USER_NAME/tmp \
--class org.bsc.dcc.vcv.ExecuteQueriesConcurrentSpark \
--master spark://namenodecontainer:7077 --deploy-mode client \
/project/targetspark/client-1.0-SNAPSHOT.jar \
/data results plans /project/targetspark/client-1.0-SNAPSHOT.jar 2 1954"                      


