#!/bin/bash

#Test query execution with Spark.
#$1 Optional argument denoting a single query to execute (e.g. query3.sql).

#Get the user name of the user executing this script.
USER_NAME=$(whoami)

#Run as a subshell inside the USER_NAME home directory because the metastore is stored there.
#The command is executed in docker as the default root user because otherwise some directories are not created.
#Note that the master used is local. 

docker exec -ti  namenodecontainer  /bin/bash -c \
"( cd /home/$USER_NAME ; /opt/spark-2.4.0-bin-hadoop2.7/bin/spark-submit --conf spark.eventLog.enabled=true  \
--packages org.apache.logging.log4j:log4j-api:2.8.2,org.apache.logging.log4j:log4j-core:2.8.2,\
org.apache.zookeeper:zookeeper:3.4.6 \
--conf spark.local.dir=/home/$USER_NAME/tmp \
--conf spark.eventLog.dir=/home/$USER_NAME/tmp \
--class org.bsc.dcc.vcv.ExecuteQueriesSpark \
--master local \
/project/targetspark/client-1.0-SNAPSHOT.jar \
/data results plans /project/targetspark/client-1.0-SNAPSHOT.jar spark $1 )"

	
