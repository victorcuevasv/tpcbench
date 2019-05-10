#!/bin/bash  

#Variables for console output with colors.

red=$'\e[1;31m'
grn=$'\e[1;32m'
yel=$'\e[1;33m'
blu=$'\e[1;34m'
mag=$'\e[1;35m'
cyn=$'\e[1;36m'
end=$'\e[0m'

#Execute the Java project with Maven on the client builder container running in the docker-compose setup. 

if [ $# -lt 2 ]; then
    echo "${yel}Usage bash runclient_executequeriesconcurrentsparkjdbc.sh <scale factor> <number of streams>${end}"
    exit 0
fi

docker exec -ti  clientbuildercontainer  /bin/bash -c \
	"mvn exec:java -Dexec.mainClass=\"org.bsc.dcc.vcv.ExecuteQueriesConcurrent\" \
	-Dexec.args=\"/data QueriesSpark results plans sparkjdbc namenodecontainer $2 1954 true true true tpcdsdb$1gb\" \
	-f /project/pomSparkJDBC.xml"       

