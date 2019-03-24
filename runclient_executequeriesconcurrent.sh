#!/bin/bash  

#Execute the Java project with Maven on the client builder container running in the docker-compose setup. 

#$1 Required argument denoting the number of streams. 

if [ $# -lt 1 ]; then
    echo "Usage bash runclient_executequeriesconcurrent.sh <number of streams>."
    exit 0
fi

docker exec -ti  clientbuildercontainer  /bin/bash -c \
	"mvn exec:java -Dexec.mainClass=\"org.bsc.dcc.vcv.ExecuteQueriesConcurrent\" \
	-Dexec.args=\"/data QueriesPresto results plans presto namenodecontainer $1 1954 false\" \
	-f /project/pom.xml"       

