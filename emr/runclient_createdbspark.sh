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
    echo "${yel}Usage: bash runclient_createdbspark.sh <scale factor>${end}"
    exit 0
fi

#Create the folder for the logs and results.

if [ ! -d /data ]; then
	sudo mkdir /data
	sudo chown hadoop:hadoop /data
fi

#Execute the Java project with Maven on the client builder container running in the docker-compose setup. 

printf "\n\n%s\n\n" "${mag}Creating and populating the database.${end}"

spark-submit --conf spark.eventLog.enabled=true  \
--packages org.apache.logging.log4j:log4j-api:2.11.2,org.apache.logging.log4j:log4j-core:2.11.2,\
org.apache.zookeeper:zookeeper:3.4.6 \
--class org.bsc.dcc.vcv.CreateDatabaseSpark \
--master yarn --deploy-mode cluster \
$DIR/../client/project/targetspark/client-1.0-SNAPSHOT.jar \
/data/tables _ext /temporal/$1GB UNUSED spark false tables s3a://tpcds-datasets/$1GB s3a://tpcds-warehouse-emr-spark-$1gb tpcdsdb$1gb $DIR/../client/project/targetspark/client-1.0-SNAPSHOT.jar     


 
