#!/bin/bash      

#Test the class that reads the .sql files from the jar.

#Add a main to the manifest.
#jar -uvfe ./client/project/targetspark/client-1.0-SNAPSHOT.jar org.bsc.dcc.vcv.JarQueriesReaderAsZipFile

#Run the main method for the class.
#java -jar ./client/project/targetspark/client-1.0-SNAPSHOT.jar ./client/project/targetspark/client-1.0-SNAPSHOT.jar QueriesSpark      

#$1 Required argument denoting the number of streams. 

#Test query execution with Spark.

#Get the user name of the user executing this script.
USER_NAME=$(whoami)
#Get the user id of the user executing this script.
USER_ID=$(id -u)
#Get the user id of the user executing this script.
GROUP_ID=$(id -g)

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
--master spark://namenodecontainer:7077 --deploy-mode cluster \
hdfs://namenodecontainer:9000/project/targetsparkcluster/client-1.0-SNAPSHOT-jar-with-dependencies.jar \
/data results plans \
hdfs://namenodecontainer:9000/project/targetsparkcluster/client-1.0-SNAPSHOT-jar-with-dependencies.jar \
spark $1 1954 true true" 


                     
