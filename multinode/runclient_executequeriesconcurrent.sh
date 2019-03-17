#!/bin/bash

#Execute the Java project with Maven on the buildhiveclient container running in docker-compose. 

docker exec -ti  clientbuildercontainer  /bin/bash -c \
	"mvn exec:java -Dexec.mainClass=\"org.bsc.dcc.vcv.ExecuteQueriesConcurrent\" \
	-Dexec.args=\"/data QueriesPresto results plans presto namenodecontainer 2 1954\" \
	-f /project/pom.xml"

