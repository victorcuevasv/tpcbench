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

#Create the ivyvolroot directory if it does not exist.

if [ ! -d $DIR/ivyvolroot ]; then
   mkdir $DIR/ivyvolroot
fi

#Create the ivyvoluser directory if it does not exist.

if [ ! -d $DIR/ivyvoluser ]; then
   mkdir $DIR/ivyvoluser
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

#Build the Hadoop base image.
printf "\n\n%s\n\n" "${mag}Building the hadoop base image.${end}"
bash server/buildHadoop.sh
buildFlags[$index]=$?
buildLabels[$index]=hadoop_base
index=$((index+1))

#Build the Presto namenode server image.
printf "\n\n%s\n\n" "${mag}Building the Presto namenode server image.${end}"
bash server/buildPresto.sh $USER_NAME $USER_ID $GROUP_ID
buildFlags[$index]=$?
buildLabels[$index]=presto_namenode
index=$((index+1))

#Build the Presto slave server image.
printf "\n\n%s\n\n" "${mag}Building the Presto slave server image.${end}"
bash server/buildSlavesPresto.sh
buildFlags[$index]=$?
buildLabels[$index]=presto_slave
index=$((index+1))

#Build the Spark namenode server image.
printf "\n\n%s\n\n" "${mag}Building the Spark namenode server image.${end}"
bash server/buildSparkMaster.sh $USER_NAME $USER_ID $GROUP_ID
buildFlags[$index]=$?
buildLabels[$index]=spark_namenode
index=$((index+1))

#Build the Spark slave server image.
printf "\n\n%s\n\n" "${mag}Building the Spark slave server image.${end}"
bash server/buildSparkSlave.sh
buildFlags[$index]=$?
buildLabels[$index]=spark_slave
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






