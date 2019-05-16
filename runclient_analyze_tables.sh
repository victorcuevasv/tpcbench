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

#Get the user id of the user executing this script.
USER_ID=$(id -u)
#Get the user id of the user executing this script.
GROUP_ID=$(id -g)

#$1 scale factor (positive integer)

if [ $# -lt 1 ]; then
    echo "${yel}Usage: bash runclient_analyze_tables.sh <scale factor>${end}"
    exit 0
fi

docker exec -ti --user $USER_ID:$GROUP_ID clientbuildercontainer  /bin/bash -c \
	"mvn exec:java -Dexec.mainClass=\"org.bsc.dcc.vcv.AnalyzeTables\" \
	-Dexec.args=\"hive namenodecontainer true tpcdsdb$1gb\" \
	-f /project/pom.xml"    
	
#docker exec -ti --user $USER_ID:$GROUP_ID clientbuildercontainer  /bin/bash -c \
#	"mvn exec:java -Dexec.mainClass=\"org.bsc.dcc.vcv.AnalyzeTables\" \
#	-Dexec.args=\"spark namenodecontainer false tpcdsdb$1gb\" \
#	-f /project/pomSparkJDBC.xml"  
   

