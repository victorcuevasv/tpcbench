#!/bin/bash   

#Variables for console output with colors.

red=$'\e[1;31m'
grn=$'\e[1;32m'
yel=$'\e[1;33m'
blu=$'\e[1;34m'
mag=$'\e[1;35m'
cyn=$'\e[1;36m'
end=$'\e[0m'

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null && pwd )"

#Get the user id of the user executing this script.
USER_ID=$(id -u)
#Get the user id of the user executing this script.
GROUP_ID=$(id -g)

#Create and populate the database from the .dat files. The scale factor is passed as an argument
#and used to identify the folder that holds the data.
#$1 scale factor (positive integer)
#$2 experiment instance number (positive integer)
#$3 number of streams (positive integer)

if [ $# -lt 2 ]; then
    echo "${yel}Usage: bash runclient_fullbenchmark.sh <scale factor> <session ID>${end}"
    exit 0
fi

#Execute the Java project with Maven on the client builder container running in the docker-compose setup. 

printf "\n\n%s\n\n" "${mag}Retrieving the history.${end}"

# args[0] schema (database) name
# args[1] host name
# args[2] session ID
# args[3] output file name

docker run --network="host" --rm --user $USER_ID:$GROUP_ID --name clientbuildercontainer -ti \
--volume $DIR/../data:/data \
--volume $DIR/../client/project:/project \
--entrypoint mvn clientbuilder:dev \
exec:java -Dexec.mainClass="org.bsc.dcc.vcv.SnowflakeHistory" \
-Dexec.args="tpcdsdb$1gb zua56993.snowflakecomputing.com $2 /data/history.log" \
-f /project/pom.xml




