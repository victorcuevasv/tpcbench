#!/bin/bash  

#Execute the Java project with Maven on the client builder container running in the docker-compose setup. 

docker exec -ti  clientbuildercontainer  /bin/bash -c \
	"mvn exec:java -Dexec.mainClass=\"org.bsc.dcc.vcv.ExecuteQueriesConcurrent\" -Dexec.args=\"/data QueriesPresto results plans presto prestohiveservercontainer 2 1954 false\" -f /project/pom.xml"       

