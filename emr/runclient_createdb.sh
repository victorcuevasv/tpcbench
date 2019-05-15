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
    echo "${yel}Usage: bash runclient_createdb.sh <scale factor>${end}"
    exit 0
fi

#Execute the Java project with Maven on the client builder container running in the docker-compose setup. 

printf "\n\n%s\n\n" "${mag}Creating and populating the database.${end}"

#First create the warehouse directory in hdfs, which (in a local cluster) is strictly necessary for presto.
#hadoop fs -mkdir -p /user/hive/warehouse

docker run --network="host" --rm --user $USER_ID:$GROUP_ID --name clientbuildercontainer -ti \
--volume $DIR/../vols/data:/data \
--volume $DIR/../client/project:/project \
--entrypoint mvn clientbuilder:dev \
	exec:java -Dexec.mainClass="org.bsc.dcc.vcv.CreateDatabase" \
	-Dexec.args="/data/tables _ext /temporal/$1GB $(hostname) prestoemr false s3a://tpcds-datasets/$1GB s3a://tpcds-warehouse-emr-presto-$1gb orc tpcdsdb$1gb" \
	-f /project/pom.xml       
  

  