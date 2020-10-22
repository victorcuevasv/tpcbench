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

#PARAMETERS.
#$1 scale factor (positive integer)

if [ $# -lt 1 ]; then
    echo "${yel}Usage: bash CreateSQLFiles.sh <scale factor>${end}"
    exit 0
fi

#Create SQL create table statement files and the query files.

#The runclient_processcreatescript script uses the java ProcessCreateScript class.
#Compile the java classes for the client first.
bash $DIR/compileCreateScript.sh

#First separate the multiple create table statements in the tpcds.sql file into separate files.
#Copy the file with create table statements into the datavol directory.

cp $DIR/dqgen2/tpcdsvarchar.sql $DIR/vols/data/tpcdsvarchar.sql
printf "\n\n%s\n\n" "${mag}Processing the tpcds[varchar].sql file.${end}"
bash $DIR/runclient_processcreatescript.sh $USER_ID $GROUP_ID tpcdsvarchar.sql
cp -r $DIR/vols/data/tables $DIR/client/project/src/main/resources/

#Generate the unused Netezza queries.
printf "\n\n%s\n\n" "${mag}Generating the Netezza queries.${end}"
bash $DIR/dqgen2/generateQueries.sh $USER_ID $GROUP_ID $1
cp -r $DIR/vols/data/QueriesNetezza $DIR/client/project/src/main/resources/

#Generate the Presto queries.
printf "\n\n%s\n\n" "${mag}Generating the Presto queries.${end}"
bash $DIR/dqgen2/generateQueriesPresto.sh $USER_ID $GROUP_ID $1
cp -r $DIR/vols/data/QueriesPresto $DIR/client/project/src/main/resources/

#Generate the Spark queries.
printf "\n\n%s\n\n" "${mag}Generating the Spark queries.${end}"
bash $DIR/dqgen2/generateQueriesSpark.sh $USER_ID $GROUP_ID $1
cp -r $DIR/vols/data/QueriesSpark $DIR/client/project/src/main/resources/

#Generate the Snowflake queries.
printf "\n\n%s\n\n" "${mag}Generating the Snowflake queries.${end}"
bash $DIR/dqgen2/generateQueriesSnowflake.sh $USER_ID $GROUP_ID $1
cp -r $DIR/vols/data/QueriesSnowflake $DIR/client/project/src/main/resources/

#Generate the Redshift queries.
printf "\n\n%s\n\n" "${mag}Generating the Redshift queries.${end}"
bash $DIR/dqgen2/generateQueriesRedshift.sh $USER_ID $GROUP_ID $1
cp -r $DIR/vols/data/QueriesRedshift $DIR/client/project/src/main/resources/

#Generate the Synapse queries.
printf "\n\n%s\n\n" "${mag}Generating the Synapse queries.${end}"
bash $DIR/dqgen2/generateQueriesSynapse.sh $USER_ID $GROUP_ID $1
cp -r $DIR/vols/data/QueriesSynapse $DIR/client/project/src/main/resources/

#Generate the BigQuery queries.
printf "\n\n%s\n\n" "${mag}Generating the BigQuery queries.${end}"
bash $DIR/dqgen2/generateQueriesBigQuery.sh $USER_ID $GROUP_ID $1
cp -r $DIR/vols/data/QueriesBigQuery $DIR/client/project/src/main/resources/




