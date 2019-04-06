#!/bin/bash   

#Variables for console output with colors.

red=$'\e[1;31m'
grn=$'\e[1;32m'
yel=$'\e[1;33m'
blu=$'\e[1;34m'
mag=$'\e[1;35m'
cyn=$'\e[1;36m'
end=$'\e[0m'

#Get the user name of the user executing this script.
USER_NAME=$(whoami)
#Get the user id of the user executing this script.
USER_ID=$(id -u)
#Get the user id of the user executing this script.
GROUP_ID=$(id -g)

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null && pwd )"

#Create and populate the database from the .dat files. The scale factor is passed as an argument
#and used to identify the folder that holds the data.
#$1 scale factor (positive integer)

if [ $# -lt 1 ]; then
    echo "Usage: bash runclient_createdbspark.sh <scale factor>."
    exit 0
fi

#Default ontainer to execute the command over, can be overriden by an argument.
CONTAINER=namenodecontainer
if [ $# -gt 1 ]; then
    CONTAINER=$2
fi

#Execute the Java project with Maven on the client builder container running in the docker-compose setup. 

printf "\n\n%s\n\n" "${mag}Creating and populating the database.${end}"

spark-submit --conf spark.eventLog.enabled=true  \
--packages org.apache.logging.log4j:log4j-api:2.8.2,org.apache.logging.log4j:log4j-core:2.8.2,\
org.apache.zookeeper:zookeeper:3.4.6 \
--conf spark.local.dir=/home/$USER_NAME/tmp \
--conf spark.eventLog.dir=/home/$USER_NAME/tmp \
--class org.bsc.dcc.vcv.CreateDatabaseSpark \
--master yarn --deploy-mode client \
../client/project/targetspark/client-1.0-SNAPSHOT.jar \
/data/tables _ext /temporal/$1GB $CONTAINER spark true tables $DIR/../client/project/targetspark/client-1.0-SNAPSHOT.jar     


 
