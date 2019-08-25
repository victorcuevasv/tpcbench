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

if [ $# -lt 3 ]; then
    echo "${yel}Usage bash runclient_executequeriesconcurrent.sh <scale factor> <experiment instance number> <number of streams>${end}"       
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
#args[14] number of streams
 
#args[15] random seed
#args[16] use multiple connections (true|false)

docker run --network="host" --rm --user $USER_ID:$GROUP_ID --name clientbuildercontainer -ti \
--volume /mnt/efs/data:/data \
--volume /mnt/efs/FileStore:/FileStore \
--volume $DIR/../client/project:/project \
--entrypoint mvn clientbuilder:dev \
exec:java -Dexec.mainClass="org.bsc.dcc.vcv.ExecuteQueriesConcurrent" \
-Dexec.args="/data tpcdsdb$1gb_$2 13ox7IwkFEcRU61h2NXeAaSZMyTRzCby8 prestosinglenode prestoemr \
tput $2 QueriesPresto results plans \
true true $(hostname) /project/target/client-1.0-SNAPSHOT.jar $3 \
1954 true" \
-f /project/pom.xml   


