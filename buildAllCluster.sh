#!/bin/bash

#Variables for console output with colors.

red=$'\e[1;31m'
grn=$'\e[1;32m'
yel=$'\e[1;33m'
blu=$'\e[1;34m'
mag=$'\e[1;35m'
cyn=$'\e[1;36m'
end=$'\e[0m'

#Generate the unused Netezza queries.
printf "\n\n%s\n\n" "${cyn}Executing the buildAll.sh script.${end}"
bash buildAll.sh

#Set permissions for data volume and hive volume.
chmod -R 777 metastorevol
chmod -R 777 warehousevol
#Permissions on datavol needed only for running Spark on YARN.
chmod -R 777 datavol


