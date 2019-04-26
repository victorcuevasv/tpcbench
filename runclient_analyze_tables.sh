#!/bin/bash

#Execute the Java project with Maven on the buildhiveclient container running in docker-compose. 

docker exec -ti  clientbuildercontainer  /bin/bash -c \
	"mvn exec:java -Dexec.mainClass=\"org.bsc.dcc.vcv.AnalyzeTables\" \
	-Dexec.args=\"hive namenodecontainer\" \
	-f /project/pom.xml"      

