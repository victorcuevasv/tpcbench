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

#$1 Required argument denoting the number of streams. 

if [ $# -lt 1 ]; then
    echo "${yel}Usage bash runclient_executequeriesconcurrent.sh <number of streams>${end}"
    exit 0
fi

docker exec -ti  clientbuildercontainer  /bin/bash -c \
	"mvn exec:java -Dexec.mainClass=\"org.bsc.dcc.vcv.ExecuteQueriesConcurrent\" \
	-Dexec.args=\"/data QueriesPresto results plans presto namenodecontainer $1 1954 false true true\" \
	-f /project/pom.xml"       

