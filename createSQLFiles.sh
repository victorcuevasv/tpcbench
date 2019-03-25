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

#Create SQL create table statement files and the query files.

#The runclient_processcreatescript script uses the java ProcessCreateScript class.
#Compile the java classes for the client first.
bash $DIR/compileclient.sh

#First separate the multiple create table statements in the tpcds.sql file into separate files.
#Copy the file with create table statements into the datavol directory.
cp $DIR/dqgen/v2.10.1rc3/tools/tpcds.sql $DIR/datavol
printf "\n\n%s\n\n" "${mag}Processing the tpcds.sql file.${end}"
bash $DIR/runclient_processcreatescript.sh $USER_ID $GROUP_ID

#Generate the unused Netezza queries.
printf "\n\n%s\n\n" "${mag}Generating the Presto queries.${end}"
bash $DIR/dqgen/generateQueries.sh $USER_ID $GROUP_ID

#Generate the Presto queries.
printf "\n\n%s\n\n" "${mag}Generating the Presto queries.${end}"
bash $DIR/dqgen/generateQueriesPresto.sh $USER_ID $GROUP_ID

#Generate the Spark queries.
printf "\n\n%s\n\n" "${mag}Generating the Spark queries.${end}"
bash $DIR/dqgen/generateQueriesSpark.sh $USER_ID $GROUP_ID
cp -r $DIR/datavol/QueriesSpark $DIR/client/project/src/main/resources/


