#!/bin/bash

#Test query execution with Spark using spark-submit.

#Get the user id of the user executing this script.
USER_ID=$(id -u)
#Get the user id of the user executing this script.
GROUP_ID=$(id -g)

docker exec  mastercontainer  /bin/bash -c \
	"/opt/spark-2.4.0-bin-hadoop2.7/bin/spark-submit \
	--conf spark.eventLog.enabled=true \
	--packages org.apache.logging.log4j:log4j-api:2.8.2,org.apache.logging.log4j:log4j-core:2.8.2 \
	--class org.bsc.dcc.vcv.ExecuteQueriesSpark \
	--master yarn --deploy-mode client \
	/project/targetspark/client-1.0-SNAPSHOT.jar \
	/data results plans /project/targetspark/client-1.0-SNAPSHOT.jar"               
	
#docker exec -ti  namenodecontainer  /bin/bash -c "/opt/spark-2.4.0-bin-hadoop2.7/bin/spark-submit 	
#--conf spark.eventLog.enabled=true  --driver-memory 8g --executor-memory 4g --num-executors 4
# --packages org.apache.logging.log4j:log4j-api:2.11.1,org.apache.logging.log4j:log4j-core:2.11.1 
# --class org.bsc.dcc.vcv.ExecuteQueriesSpark --master yarn --deploy-mode client 
# /project/targetspark/client-1.0-SNAPSHOT.jar /data results plans 
# /project/targetspark/client-1.0-SNAPSHOT.jar query2.sql"    

 