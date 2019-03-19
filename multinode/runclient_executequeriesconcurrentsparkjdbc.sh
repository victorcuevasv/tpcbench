#!/bin/bash 

#Execute the Java project with Maven on the buildhiveclient container running in docker-compose. 

docker exec -ti  clientbuildercontainer  /bin/bash -c \
	"mvn exec:java -Dexec.mainClass=\"org.bsc.dcc.vcv.ExecuteQueriesConcurrent\" \
	-Dexec.args=\"/data QueriesSpark results plans sparkjdbc namenodecontainer 2 1954 true\" \
	-f /project/pomSparkJDBC.xml"

