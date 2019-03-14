#!/bin/bash

#Execute the Java project with Maven on the client builder container running in the docker-compose setup. 

docker exec -ti  clientbuildercontainer  /bin/bash -c \
	"mvn exec:java -Dexec.mainClass=\"org.bsc.dcc.vcv.ExecuteQueries\" \
	-Dexec.args=\"/data QueriesPresto results plans presto mastercontainer query3.sql \" -f /project/pom.xml"      

