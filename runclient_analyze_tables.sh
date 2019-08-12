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

if [ $# -lt 2 ]; then
    echo "${yel}Usage: bash runclient_analyze_tables.sh <scale factor> <experiment instance number>${end}"
    exit 0
fi

#args[0] main work directory
#args[1] schema (database) name
#args[2] results folder name (e.g. for Google Drive)
#args[3] experiment name (name of subfolder within the results folder)
#args[4] system name (system name used within the logs)

#args[5] test name (i.e. load)
#args[6] experiment instance number
#args[7] compute statistics for columns (true/false)
#args[8] hostname of the server

docker exec -ti --user $USER_ID:$GROUP_ID clientbuildercontainer  /bin/bash -c \
"mvn exec:java -Dexec.mainClass=\"org.bsc.dcc.vcv.AnalyzeTables\" \
-Dexec.args=\"data tpcdsdb$1gb 13ox7IwkFEcRU61h2NXeAaSZMyTRzCby8 prestosinglenode hive \
analyze $2 true namenodecontainer \" \
-f /project/pom.xml"    


