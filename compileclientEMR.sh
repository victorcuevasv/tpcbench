#!/bin/bash

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

#Build the client project.
printf "\n\n%s\n\n" "${blu}Compiling the client EMR project.${end}"
bash client/compileEMR.sh $USER_ID $GROUP_ID

#cp client/project/targetemr/client-1.2-SNAPSHOT-SHADED.jar $HOME/tpcds-jars/targetemr/client-1.2-SNAPSHOT-SHADED.jar  

