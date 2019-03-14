#!/bin/bash
#Execute the Java project with Maven by running the container (standalone container, no docker-compose). Can fail due to being unable to resolve localhost to the hiveservercontainer.

#docker run --rm -v $(pwd)/project:/project  --entrypoint mvn buildhiveclient:dev exec:java -Dexec.mainClass="org.bsc.dcc.vcv.ProcessCreateScript" -Dexec.args="/data tpcds.sql" -f /project/pom.xml   

#Execute the Java project with Maven on the buildhiveclient container running in docker-compose. 

#docker exec -ti  hiveclientcontainer  /bin/bash -c "mvn exec:java -Dexec.mainClass=\"org.bsc.dcc.vcv.ProcessCreateScript\" -Dexec.args=\"/data tpcds.sql\" -f /project/pom.xml"

#docker exec -ti  sparkhiveclientcontainer  /bin/bash -c "mvn exec:java -Dexec.mainClass=\"org.bsc.dcc.vcv.JavaSparkHiveExample\" -Dexec.args=\"\" -f /project/pomSpark.xml"

#The client jar file must be copied to the temporal volume in the server container.

#docker exec -ti  sparkhiveservercontainer  /bin/bash -c "/opt/spark-2.4.0-bin-hadoop2.7/bin/spark-submit --class org.bsc.dcc.vcv.JavaSparkHiveExample --master spark://sparkhiveservercontainer:7077 --deploy-mode client /temporal/client-1.0-SNAPSHOT.jar"       

#Test ResourceWalker.

#docker exec -ti  sparkhiveclientcontainer  /bin/bash -c "mvn exec:java -Dexec.mainClass=\"org.bsc.dcc.vcv.JarQueriesReader\" -Dexec.args=\"\" -f /project/pomSpark.xml"

#Test query execution with Spark.

docker exec -ti  mastercontainer  /bin/bash -c \
	"/opt/spark-2.4.0-bin-hadoop2.7/bin/spark-submit \
	--packages org.apache.logging.log4j:log4j-api:2.8.2,org.apache.logging.log4j:log4j-core:2.8.2 \
	--class org.bsc.dcc.vcv.ExecuteQueriesSpark \
	--master spark://mastercontainer:7077 --deploy-mode client \
	/project/targetspark/client-1.0-SNAPSHOT.jar \
	/data results plans /project/targetspark/client-1.0-SNAPSHOT.jar"               

#mvn exec:java -Dexec.mainClass="org.bsc.dcc.vcv.JarQueriesReaderAsZipFile" -Dexec.args="client/project/target/client-1.0-SNAPSHOT.jar" -f client/project/pomSpark.xml
