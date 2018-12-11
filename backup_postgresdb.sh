#!/bin/bash

red=$'\e[1;31m'
grn=$'\e[1;32m'
yel=$'\e[1;33m'
blu=$'\e[1;34m'
mag=$'\e[1;35m'
cyn=$'\e[1;36m'
end=$'\e[0m'

#Backup the postgres .
printf "\n\n%s\n\n" "${blu}Backing up postgres database.${end}"
#Use the line below for a backup with a time snapshot.
#docker exec -t postgrescontainer pg_dumpall -c -U root > dump_`date +%d-%m-%Y"_"%H_%M_%S`.sql 
docker exec -t postgrescontainer pg_dumpall -c -U root > dump_metastore.sql 

