#!/bin/bash

#Test query execution with Spark.

#Get the user id of the user executing this script.
USER_ID=$(id -u)
#Get the user id of the user executing this script.
GROUP_ID=$(id -g)

docker exec --user $USER_ID:$GROUP_ID -ti  namenodecontainer  /bin/bash -c \
	"/opt/spark-2.4.0-bin-hadoop2.7/bin/spark-submit --conf spark.eventLog.enabled=true  \
	--packages org.apache.logging.log4j:log4j-api:2.8.2,\
	org.apache.logging.log4j:log4j-core:2.8.2,org.apache.zookeeper:zookeeper:3.4.6 \
	--class org.bsc.dcc.vcv.ExecuteQueriesConcurrentSpark \
	--master spark://namenodecontainer:7077 --deploy-mode client \
	/project/targetspark/client-1.0-SNAPSHOT.jar \
	/data results plans /project/targetspark/client-1.0-SNAPSHOT.jar 2 1954"                      


