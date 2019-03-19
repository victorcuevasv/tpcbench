#!/bin/bash

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null && pwd )"

#Variables for console output with colors.

red=$'\e[1;31m'
grn=$'\e[1;32m'
yel=$'\e[1;33m'
blu=$'\e[1;34m'
mag=$'\e[1;35m'
cyn=$'\e[1;36m'
end=$'\e[0m'

#Create the datavol directory if it does not exist.

if [ ! -d $DIR/datavol ]; then
   mkdir $DIR/datavol
fi

#Create the hivevol on the PARENT directory if it does not exist.

if [ ! -d $DIR/../hivevol ]; then
   mkdir $DIR/../hivevol
fi

#Create the metastorevol directory if it does not exist.

if [ ! -d $DIR/metastorevol ]; then
   mkdir $DIR/metastorevol
fi
   
#Create the warehousevol directory if it does not exist.

if [ ! -d $DIR/warehousevol ]; then
   mkdir $DIR/warehousevol
fi

#Get the username the user executing this script.
USER_NAME=$(whoami)
#Get the user id of the user executing this script.
USER_ID=$(id -u)
#Get the user id of the user executing this script.
GROUP_ID=$(id -g)

#Build the namenode server image.
printf "\n\n%s\n\n" "${mag}Building the namenode server image.${end}"
bash server/buildPresto.sh

#Build the slave server image.
printf "\n\n%s\n\n" "${mag}Building the slave server image.${end}"
bash server/buildSlavesPresto.sh

#Build the namenode server image.
printf "\n\n%s\n\n" "${mag}Building the namenode server image.${end}"
bash server/buildSparkMaster.sh $USER_NAME $USER_ID $GROUP_ID

#Build the slave server image.
printf "\n\n%s\n\n" "${mag}Building the slave server image.${end}"
bash server/buildSparkSlave.sh




