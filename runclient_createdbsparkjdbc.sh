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

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null && pwd )"

#Create and populate the database from the .dat files. The scale factor is passed as an argument
#and used to identify the folder that holds the data.
#$1 scale factor (positive integer)

if [ $# -lt 1 ]; then
    echo "${yel}Usage: bash runclient_createdbsparkjdbc.sh <scale factor>${end}"
    exit 0
fi

#Default ontainer to execute the command over, can be overriden by an argument.
CONTAINER=namenodecontainer
if [ $# -gt 1 ]; then
    CONTAINER=$2
fi

#Execute the Java project with Maven on the client builder container running in the docker-compose setup. 

printf "\n\n%s\n\n" "${mag}Creating and populating the database.${end}"

docker exec -ti --user $USER_ID:$GROUP_ID clientbuildercontainer  /bin/bash -c \
"mvn exec:java -Dexec.mainClass=\"org.bsc.dcc.vcv.CreateDatabase\" \
-Dexec.args=\"/data/tables _ext temporal/$1GB $CONTAINER spark false \
hdfs://namenodecontainer:9000 hdfs://namenodecontainer:9000/user/hive/warehouse tpcdsdb$1gb\" \
-f /project/pomSparkJDBC.xml"       
  


