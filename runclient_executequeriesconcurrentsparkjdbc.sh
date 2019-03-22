#!/bin/bash  

#Execute the Java project with Maven on the client builder container running in the docker-compose setup. 

#$1 Required argument denoting the number of streams. 

if [ $# -lt 1 ]; then
    echo "Usage bash runclient_executequeriesconcurrentsparkjdbc.sh <number of streams>."
    exit 0
fi

docker exec -ti  clientbuildercontainer  /bin/bash -c \
	"mvn exec:java -Dexec.mainClass=\"org.bsc.dcc.vcv.ExecuteQueriesConcurrent\" \
	-Dexec.args=\"/data QueriesSpark results plans sparkjdbc mastercontainer $1 1954 true\" \
	-f /project/pomSparkJDBC.xml"       

