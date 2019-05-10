#!/bin/bash   

#Variables for console output with colors.

red=$'\e[1;31m'
grn=$'\e[1;32m'
yel=$'\e[1;33m'
blu=$'\e[1;34m'
mag=$'\e[1;35m'
cyn=$'\e[1;36m'
end=$'\e[0m'

#Execute the Java project with Maven on the buildhiveclient container running in docker-compose. 

if [ $# -lt 2 ]; then
    echo "${yel}Usage: bash runclient_executequeriessparkjdbc.sh <scale factor> <all | query filename>${end}"
    exit 0
fi

docker exec -ti  clientbuildercontainer  /bin/bash -c \
	"mvn exec:java -Dexec.mainClass=\"org.bsc.dcc.vcv.ExecuteQueries\" \
	-Dexec.args=\"/data QueriesSpark results plans sparkjdbc namenodecontainer true true tpcdsdb$1gb $2\" \
	-f /project/pomSparkJDBC.xml"    

