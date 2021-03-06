#!/bin/bash

#Variables for console output with colors.

red=$'\e[1;31m'
grn=$'\e[1;32m'
yel=$'\e[1;33m'
blu=$'\e[1;34m'
mag=$'\e[1;35m'
cyn=$'\e[1;36m'
end=$'\e[0m'

#Get the user id of the user executing this script.
USER_ID=$(id -u)
#Get the user id of the user executing this script.
GROUP_ID=$(id -g)

#Execute the Java project with Maven on the buildhiveclient container running in docker-compose. 

if [ $# -lt 3 ]; then
    echo "${yel}Usage: bash runclient_executequeries.sh <scale factor> <experiment instance number> <all | query filename>${end}"     
    exit 0
fi

#args[0] main work directory
#args[1] schema (database) name
#args[2] results folder name (e.g. for Google Drive)
#args[3] experiment name (name of subfolder within the results folder)
#args[4] system name (system name used within the logs)
 
#args[5] test name (e.g. power)
#args[6] experiment instance number
#args[7] queries dir
#args[8] subdirectory of work directory to store the results
#args[9] subdirectory of work directory to store the execution plans
 
#args[10] save plans (boolean)
#args[11] save results (boolean)
#args[12] hostname of the server
#args[13] jar file
#args[14] "all" or query file

docker exec -ti --user $USER_ID:$GROUP_ID clientbuildercontainer  /bin/bash -c \
"mvn exec:java -Dexec.mainClass=\"org.bsc.dcc.vcv.ExecuteQueries\" \
-Dexec.args=\"/data tpcdsdb$1gb 13ox7IwkFEcRU61h2NXeAaSZMyTRzCby8 prestosinglenode presto \
power $2 QueriesPresto results plans \
true true namenodecontainer /project/target/client-1.0-SNAPSHOT.jar $3\" \
-f /project/pom.xml"



