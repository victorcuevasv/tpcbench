#!/bin/bash

#Test query execution with Spark.
#$1 Optional argument denoting a single query to execute (e.g. query3.sql).

#Get the user name of the user executing this script.
USER_NAME=$(whoami)
#Get the user id of the user executing this script.
USER_ID=$(id -u)
#Get the user id of the user executing this script.
GROUP_ID=$(id -g)

docker exec --user $USER_ID:$GROUP_ID -ti  namenodecontainer  /bin/bash -c \
"/opt/spark-2.4.0-bin-hadoop2.7/bin/spark-submit --conf spark.eventLog.enabled=true  \
--packages org.apache.logging.log4j:log4j-api:2.11.2,org.apache.logging.log4j:log4j-core:2.11.2,\
org.apache.zookeeper:zookeeper:3.4.6 \
--conf spark.local.dir=/home/$USER_NAME/tmp \
--conf spark.eventLog.dir=/home/$USER_NAME/tmp \
--class org.bsc.dcc.vcv.JavaSparkPiExample \
--master spark://namenodecontainer:7077 --deploy-mode cluster \
hdfs://namenodecontainer:9000/project/targetsparkexample/client-1.0-SNAPSHOT-jar-with-dependencies.jar  \
/data results plans \
hdfs://namenodecontainer:9000/project/targetsparkexample/client-1.0-SNAPSHOT-jar-with-dependencies.jar  \
4"                



