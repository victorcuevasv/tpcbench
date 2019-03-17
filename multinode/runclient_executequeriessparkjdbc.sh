#!/bin/bash   

#Execute the Java project with Maven on the buildhiveclient container running in docker-compose. 

docker exec -ti  clientbuildercontainer  /bin/bash -c \
	"mvn exec:java -Dexec.mainClass=\"org.bsc.dcc.vcv.ExecuteQueries\" \
	-Dexec.args=\"/data QueriesPresto results plans spark namenodecontainer query2.sql\" \
	-f /project/pomSparkJDBC.xml"

