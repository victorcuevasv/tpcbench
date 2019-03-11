#!/bin/bash

#Variables for console output with colors.

red=$'\e[1;31m'
grn=$'\e[1;32m'
yel=$'\e[1;33m'
blu=$'\e[1;34m'
mag=$'\e[1;35m'
cyn=$'\e[1;36m'
end=$'\e[0m'

#Create SQL create table statement files and the query files.

#First separate the multiple create table statements in the tpcds.sql file into separate files.
printf "\n\n%s\n\n" "${mag}Processing the tpcds.sql file.${end}"
bash runclient_processcreatescript.sh

#Generate the Presto queries.
printf "\n\n%s\n\n" "${mag}Generating the Presto queries.${end}"
bash dqgen/generateQueriesPresto.sh
cp -r dqgen/output/QueriesPresto ./datavol

#Generate the Spark queries.
printf "\n\n%s\n\n" "${mag}Generating the Spark queries.${end}"
bash dqgen/generateQueriesSpark.sh
cp -r dqgen/output/QueriesSpark ./datavol

