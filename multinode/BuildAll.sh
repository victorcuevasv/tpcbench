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

#Get the time of start of execution to measure total execution time.
start_time=`date +%s`

#Create the volume directories in the PARENT directory.
bash $DIR/../createDirs.sh

#Get the username the user executing this script.
USER_NAME=$(whoami)
#Get the user id of the user executing this script.
USER_ID=$(id -u)
#Get the user id of the user executing this script.
GROUP_ID=$(id -g)

buildFlags=()
buildLabels=()
index=0


#Build the Ubuntu with java base image.
printf "\n\n%s\n\n" "${mag}Building the ubuntu with java image.${end}"
bash $DIR/../ubuntujava/build.sh
buildFlags[$index]=$?
buildLabels[$index]=ubuntujava
index=$((index+1))

#Build the Hadoop base image.
printf "\n\n%s\n\n" "${mag}Building the hadoop base image.${end}"
bash $DIR/server/buildHadoop.sh
buildFlags[$index]=$?
buildLabels[$index]=hadoop_base
index=$((index+1))

#Build the Presto namenode server image.
printf "\n\n%s\n\n" "${mag}Building the Presto namenode server image.${end}"
bash $DIR/server/buildPresto.sh $USER_NAME $USER_ID $GROUP_ID
buildFlags[$index]=$?
buildLabels[$index]=presto_namenode
index=$((index+1))

#Build the Presto slave server image.
printf "\n\n%s\n\n" "${mag}Building the Presto slave server image.${end}"
bash $DIR/server/buildSlavesPresto.sh
buildFlags[$index]=$?
buildLabels[$index]=presto_slave
index=$((index+1))

#Build the Spark namenode server image.
printf "\n\n%s\n\n" "${mag}Building the Spark namenode server image.${end}"
bash $DIR/server/buildSparkMaster.sh $USER_NAME $USER_ID $GROUP_ID
buildFlags[$index]=$?
buildLabels[$index]=spark_namenode
index=$((index+1))

#Build the Spark slave server image.
printf "\n\n%s\n\n" "${mag}Building the Spark slave server image.${end}"
bash $DIR/server/buildSparkSlave.sh
buildFlags[$index]=$?
buildLabels[$index]=spark_slave
index=$((index+1))

#Build the client image.
printf "\n\n%s\n\n" "${mag}Building the client image.${end}"
bash $DIR/../client/buildSingle.sh $USER_NAME $USER_ID $GROUP_ID
buildFlags[$index]=$?
buildLabels[$index]=client
index=$((index+1))

#Build the dqgen image with the TPC-DS toolkit to generate data and queries.
printf "\n\n%s\n\n" "${mag}Creating the dqgen TPC-DS toolkit image.${end}"
bash $DIR/../dqgen/createContainer.sh $USER_NAME $USER_ID $GROUP_ID
buildFlags[$index]=$?
buildLabels[$index]=tpcds
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









