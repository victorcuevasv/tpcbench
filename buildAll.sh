#!/bin/bash

#Variables for console output with colors.

red=$'\e[1;31m'
grn=$'\e[1;32m'
yel=$'\e[1;33m'
blu=$'\e[1;34m'
mag=$'\e[1;35m'
cyn=$'\e[1;36m'
end=$'\e[0m'

#Get the time of start of execution to measure total execution time.
start_time=`date +%s`

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

#Create the ivyrootvol directory if it does not exist.

if [ ! -d ivyrootvol ]; then
   mkdir ivyrootvol
fi

#Create the ivyrootvol directory if it does not exist.

if [ ! -d ivyuservol ]; then
   mkdir ivyuservol
fi

#Get the username the user executing this script.
USER_NAME=$(whoami)
#Get the user id of the user executing this script.
USER_ID=$(id -u)
#Get the user id of the user executing this script.
GROUP_ID=$(id -g)

buildFlags=()
buildLabels=()
index=0

#Build the Ubuntu with Java base image.
printf "\n\n%s\n\n" "${mag}Creating the Ubuntu with Java base image.${end}"
bash ubuntujava/build.sh
buildFlags[$index]=$?
buildLabels[$index]=ubuntujava
index=$((index+1))

#Build the dqgen image with the TPC-DS toolkit to generate data and queries.
printf "\n\n%s\n\n" "${mag}Creating the dqgen TPC-DS toolkit image.${end}"
bash dqgen/createContainer.sh $USER_NAME $USER_ID $GROUP_ID
buildFlags[$index]=$?
buildLabels[$index]=tpcds
index=$((index+1))

#Build the presto server image.
printf "\n\n%s\n\n" "${mag}Building the Presto server image.${end}"
bash server/buildPresto.sh $USER_NAME $USER_ID $GROUP_ID
buildFlags[$index]=$?
buildLabels[$index]=presto_server
index=$((index+1))

#Build the Spark Master server image.
printf "\n\n%s\n\n" "${mag}Building the Spark Master server image.${end}"
bash server/buildSparkMaster.sh $USER_NAME $USER_ID $GROUP_ID
buildFlags[$index]=$?
buildLabels[$index]=spark_master
index=$((index+1))

#Build the Spark Worker server image.
printf "\n\n%s\n\n" "${mag}Building the Spark Worker server image.${end}"
bash server/buildSparkWorker.sh
buildFlags[$index]=$?
buildLabels[$index]=spark_worker
index=$((index+1))

#Build the client project builder image.
printf "\n\n%s\n\n" "${mag}Building the client project builder image.${end}"
bash client/buildSingle.sh $USER_NAME $USER_ID $GROUP_ID
buildFlags[$index]=$?
buildLabels[$index]=client
index=$((index+1))

index=0
for flag in ${buildFlags[@]} ; do
   if [[ $flag -eq 0 ]]; then
   	echo "${grn}Image ${buildLabels[$index]} succeeded.${end}"
   else
   	echo "${red}Image ${buildLabels[$index]} failed.${end}"
   fi
   index=$((index+1))
done

end_time=`date +%s`

runtime=$((end_time-start_time))
printf "\n\n%s\n\n" "${cyn}Total execution time: ${runtime} sec.${end}"




