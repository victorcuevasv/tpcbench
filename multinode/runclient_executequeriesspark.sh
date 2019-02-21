#!/bin/bash

#Test query execution with Spark.

docker exec -ti  namenodecontainer  /bin/bash -c "/opt/spark-2.4.0-bin-hadoop2.7/bin/spark-submit --packages org.apache.logging.log4j:log4j-api:2.11.1,org.apache.logging.log4j:log4j-core:2.11.1 --class org.bsc.dcc.vcv.ExecuteQueriesSpark --master spark://namenodecontainer:7077 --deploy-mode client /temporal/client-1.0-SNAPSHOT.jar /temporal results plans /temporal/client-1.0-SNAPSHOT.jar"               


