#!/bin/bash  

#Execute the Java project with Maven on the client builder container running in the docker-compose setup. 

docker exec -ti  clientbuildercontainer  /bin/bash -c \
	"mvn exec:java -Dexec.mainClass=\"org.bsc.dcc.vcv.ExecuteQueriesConcurrent\" \
	-Dexec.args=\"/data QueriesSpark results plans sparkjdbc mastercontainer 2 1954 true\" \
	-f /project/pomSparkJDBC.xml"       

