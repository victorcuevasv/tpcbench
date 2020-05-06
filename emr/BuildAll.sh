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

#Get the time of start of execution to measure total execution time.
start_time=`date +%s`

#Create the volume directories.
bash $DIR/../createDirs.sh

#Get the username the user executing this script.
USER_NAME=$(whoami)
#Get the user id of the user executing this script.
USER_ID=$(id -u)
#Get the user id of the user executing this script.
GROUP_ID=$(id -g)
#Indicates whether to use the local mirror for the dependencies (0=no / 1=yes).
LOCAL_MIRROR=1

buildFlags=()
buildLabels=()
index=0

#Build the Ubuntu with Java base image.
printf "\n\n%s\n\n" "${mag}Creating the Ubuntu with Java base image.${end}"
bash $DIR/../ubuntujava/build.sh
buildFlags[$index]=$?
buildLabels[$index]=ubuntujava
index=$((index+1))

#Build the dqgen image with the TPC-DS toolkit to generate data and queries.
printf "\n\n%s\n\n" "${mag}Creating the dqgen TPC-DS toolkit image.${end}"
bash $DIR/../dqgen/createContainer.sh $USER_NAME $USER_ID $GROUP_ID
buildFlags[$index]=$?
buildLabels[$index]=tpcds
index=$((index+1))

#Build the client project builder image.
printf "\n\n%s\n\n" "${mag}Building the client project builder image.${end}"
bash $DIR/../client/buildSingle.sh $USER_NAME $USER_ID $GROUP_ID $LOCAL_MIRROR $JDBC
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




