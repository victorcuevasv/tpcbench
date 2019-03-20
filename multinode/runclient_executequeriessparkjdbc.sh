#!/bin/bash   

#Test query execution with Spark.
#$1 Optional argument denoting a single query to execute (e.g. query4.sql).

#Execute the Java project with Maven on the buildhiveclient container running in docker-compose. 

docker exec -ti  clientbuildercontainer  /bin/bash -c \
	"mvn exec:java -Dexec.mainClass=\"org.bsc.dcc.vcv.ExecuteQueries\" \
	-Dexec.args=\"/data QueriesSpark results plans sparkjdbc namenodecontainer $1\" \
	-f /project/pomSparkJDBC.xml"

