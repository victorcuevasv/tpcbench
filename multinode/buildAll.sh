#!/bin/bash

#Variables for console output with colors.

red=$'\e[1;31m'
grn=$'\e[1;32m'
yel=$'\e[1;33m'
blu=$'\e[1;34m'
mag=$'\e[1;35m'
cyn=$'\e[1;36m'
end=$'\e[0m'

#Create the datavol directory if it does not exist.

if [ ! -d datavol ]; then
   mkdir datavol
fi

#Create the hivevol directory if it does not exist.

if [ ! -d hivevol ]; then
   mkdir hivevol
fi

#Create the metastorevol directory if it does not exist.

if [ ! -d metastorevol ]; then
   mkdir metastorevol
fi
   
#Create the warehousevol directory if it does not exist.

if [ ! -d warehousevol ]; then
   mkdir warehousevol
fi

#Build the namenode server image.
printf "\n\n%s\n\n" "${mag}Building the namenode server image.${end}"
bash server/buildPresto.sh

#Build the slave server image.
printf "\n\n%s\n\n" "${mag}Building the slave server image.${end}"
bash server/buildSlavesPresto.sh

#Build the namenode server image.
printf "\n\n%s\n\n" "${mag}Building the namenode server image.${end}"
bash server/buildSparkMaster.sh

#Build the slave server image.
printf "\n\n%s\n\n" "${mag}Building the slave server image.${end}"
bash server/buildSparkSlave.sh




