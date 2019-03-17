#!/bin/bash      

#Test the class that reads the .sql files from the jar.

#Add a main to the manifest.
#jar -uvfe ./client/project/targetspark/client-1.0-SNAPSHOT.jar org.bsc.dcc.vcv.JarQueriesReaderAsZipFile

#Run the main method for the class.
#java -jar ./client/project/targetspark/client-1.0-SNAPSHOT.jar ./client/project/targetspark/client-1.0-SNAPSHOT.jar    

#Test query execution with Spark.

#Get the user id of the user executing this script.
USER_ID=$(id -u)
#Get the user id of the user executing this script.
GROUP_ID=$(id -g)

docker exec --user $USER_ID:$GROUP_ID -ti  mastercontainer  /bin/bash -c \
	"/opt/spark-2.4.0-bin-hadoop2.7/bin/spark-submit \
	--packages org.apache.logging.log4j:log4j-api:2.8.2,org.apache.logging.log4j:log4j-core:2.8.2 \
	--class org.bsc.dcc.vcv.ExecuteQueriesConcurrentSpark \
	--master spark://mastercontainer:7077 --deploy-mode client \
	/project/targetspark/client-1.0-SNAPSHOT.jar \
	/data results plans /project/targetspark/client-1.0-SNAPSHOT.jar 2 1954"                      

