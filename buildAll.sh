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
   cp dqgen/v2.10.1rc3/tools/tpcds.sql datavol
fi

#Get the username the user executing this script.
USER_NAME=$(whoami)
#Get the user id of the user executing this script.
USER_ID=$(id -u)
#Get the user id of the user executing this script.
GROUP_ID=$(id -g)

#Build the Ubuntu with Java base image.
#printf "\n\n%s\n\n" "${mag}Creating the Ubuntu with Java base image.${end}"
#bash ubuntujava/build.sh

#Build the dqgen image with the TPC-DS toolkit to generate data and queries.
#printf "\n\n%s\n\n" "${mag}Creating the dqgen TPC-DS toolkit image.${end}"
#bash dqgen/createContainer.sh

#Build the presto server image.
#printf "\n\n%s\n\n" "${mag}Building the Presto server image.${end}"
#bash server/buildPresto.sh

#Build the Spark Master server image.
#printf "\n\n%s\n\n" "${mag}Building the Spark Master server image.${end}"
#bash server/buildSparkMaster.sh

#Build the Spark Worker server image.
#printf "\n\n%s\n\n" "${mag}Building the Spark Worker server image.${end}"
#bash server/buildSparkWorker.sh

#Build the client project builder image.
printf "\n\n%s\n\n" "${mag}Building the client project builder image.${end}"
bash client/buildSingle.sh $USER_NAME $USER_ID $GROUP_ID

exit 0

#Compile the Presto/Hive JDBC client project.
printf "\n\n%s\n\n" "${blu}Compiling the Presto/Hive JDBC client project.${end}"
bash client/compile.sh

#Compile the Spark (spark-submit) client project.
printf "\n\n%s\n\n" "${blu}Compiling the Spark client project.${end}"
bash client/compileSpark.sh

#Compile the Spark (spark-submit) client project.
printf "\n\n%s\n\n" "${blu}Compiling the Spark JDBC client project.${end}"
bash client/compileSparkJDBC.sh


