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

if [ $# -lt 2 ]; then
    echo "${yel}Usage: bash runclient_createdb.sh <scale factor> <experiment instance number>${end}"
    exit 0
fi

#Execute the Java project with Maven on the client builder container running in the docker-compose setup. 

printf "\n\n%s\n\n" "${mag}Creating and populating the database.${end}"

#args[0] main work directory
#args[1] schema (database) name
#args[2] results folder name (e.g. for Google Drive)
#args[3] experiment name (name of subfolder within the results folder)
#args[4] system name (system name used within the logs)
	  
#args[5] test name (i.e. load)
#args[6] experiment instance number
#args[7] directory for generated data raw files
#args[8] subdirectory within the jar that contains the create table files
#args[9] suffix used for intermediate table text files
 
#args[10] prefix of external location for raw data tables (e.g. S3 bucket), null for none
#args[11] prefix of external location for created tables (e.g. S3 bucket), null for none
#args[12] format for column-storage tables (PARQUET, DELTA)
#args[13] whether to run queries to count the tuples generated (true/false)
#args[14] whether to use data partitioning for the tables (true/false)
 
#args[15] hostname of the server
#args[16] username for the connection
#args[17] jar file

docker run --network="host" --rm --user $USER_ID:$GROUP_ID --name clientbuildercontainer -ti \
--volume /mnt/efs/data:/data \
--volume $DIR/../client/project:/project \
--volume $DIR/../vols/hive:/temporal \
--entrypoint mvn clientbuilder:dev \
exec:java -Dexec.mainClass="org.bsc.dcc.vcv.CreateDatabase" \
-Dexec.args="/data tpcdsdb$1gb 1-rtvjs-45qnx2peo-ar39q2dprvzkmga snowflake snowflake \
load $2 /temporal/$1GB tables UNUSED \
TPCDSDB$1GB_S3_STAGE/$1GB null UNUSED true false \
zua56993.snowflakecomputing.com bsctest /project/target/client-1.0-SNAPSHOT.jar" \
-f /project/pom.xml


