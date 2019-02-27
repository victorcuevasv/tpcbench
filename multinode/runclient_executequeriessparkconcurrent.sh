#!/bin/bash

#Test query execution with Spark.

docker exec -ti  namenodecontainer  /bin/bash -c "/opt/spark-2.4.0-bin-hadoop2.7/bin/spark-submit --driver-memory 2g --executor-memory 1g --num-executors 4 --packages org.apache.logging.log4j:log4j-api:2.11.1,org.apache.logging.log4j:log4j-core:2.11.1 --class org.bsc.dcc.vcv.ExecuteQueriesConcurrentSpark --master spark://namenodecontainer:7077 --deploy-mode client /project/client-1.0-SNAPSHOT.jar /data results plans /project/client-1.0-SNAPSHOT.jar 2 1954"                      


