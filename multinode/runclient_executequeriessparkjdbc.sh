#!/bin/bash   

#Execute the Java project with Maven on the buildhiveclient container running in docker-compose. 

docker exec -ti  clientbuildercontainer  /bin/bash -c \
	"mvn exec:java -Dexec.mainClass=\"org.bsc.dcc.vcv.ExecuteQueries\" \
	-Dexec.args=\"/data QueriesSpark results plans sparkjdbc namenodecontainer query2.sql\" \
	-f /project/pomSparkJDBC.xml"

