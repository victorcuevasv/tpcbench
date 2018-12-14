#!/bin/bash

#Variables for console output with colors.

red=$'\e[1;31m'
grn=$'\e[1;32m'
yel=$'\e[1;33m'
blu=$'\e[1;34m'
mag=$'\e[1;35m'
cyn=$'\e[1;36m'
end=$'\e[0m'

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

#Create the datavol directory and its subdirectories if it does not exist.

if [ ! -d datavol ]; then
   mkdir datavol
   mkdir datavol/tables
   mkdir datavol/tablestextfile
   mkdir datavol/tablesparquet
   mkdir datavol/results
   mkdir datavol/plans
   mkdir datavol/logs
   #Copy the queries file into the datavol directory.
   cp benchmark/tpcds.sql datavol
fi

#Build the Ubuntu with Java base image.
printf "\n\n%s\n\n" "${mag}Creating Ubuntu with Java base image.${end}"
bash ubuntujava/build.sh

#Build the server image.
printf "\n\n%s\n\n" "${mag}Building the server image.${end}"
bash server/buildPresto.sh

#Build the client project builder image.
printf "\n\n%s\n\n" "${mag}Building the client project builder image.${end}"
bash client/buildPresto.sh

#Set permissions for data volume and client project.
chmod -R 777 client/project
sudo chmod -R 777 client/project
chmod -R 777 datavol
sudo chmod -R 777 datavol

#Compile the client project.
printf "\n\n%s\n\n" "${blu}Compiling the client project.${end}"
bash client/compile.sh



