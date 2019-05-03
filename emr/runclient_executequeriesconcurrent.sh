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

#Execute the Java project with Maven on the client builder container running in the docker-compose setup. 

#$1 Required argument denoting the number of streams. 

if [ $# -lt 1 ]; then
    echo "${yel}Usage bash runclient_executequeriesconcurrent.sh <number of streams>${end}"
    exit 1
fi

docker run --network="host" --rm --user $USER_ID:$GROUP_ID --name clientbuildercontainer -ti \
--volume $DIR/../vols/data:/data \
--volume $DIR/../client/project:/project \
--entrypoint mvn clientbuilder:dev \
	exec:java -Dexec.mainClass="org.bsc.dcc.vcv.ExecuteQueriesConcurrent" \
	-Dexec.args="/data QueriesPresto results plans prestoemr $(hostname) $1 1954 true true true" \
	-f /project/pom.xml   


