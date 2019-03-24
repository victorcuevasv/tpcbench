#!/bin/bash

#Execute the Java project with Maven on the buildhiveclient container running in docker-compose. 
#$1 Optional argument denoting a single query to execute (e.g. query2.sql).

docker exec -ti  clientbuildercontainer  /bin/bash -c \
	"mvn exec:java -Dexec.mainClass=\"org.bsc.dcc.vcv.ExecuteQueries\" \
	-Dexec.args=\"/data QueriesPresto results plans presto namenodecontainer $1\" \
	-f /project/pom.xml"      

