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

#Get the user id of the user executing this script.
USER_ID=$(id -u)
#Get the user id of the user executing this script.
GROUP_ID=$(id -g)

#Create .dat files to populate the database. The scale factor is passed as an argument.
#$1 scale factor (positive integer)

if [ $# -lt 2 ]; then
    printf "\n%s\n\n" "${yel}Usage bash createDataFilesPar.sh <scale factor> <degree of parallelism>${end}"
    exit 1
fi

printf "\n%s\n" "${mag}Generating the data files with parallelism.${end}"
bash $DIR/dqgen2/generateDataPar.sh $1 $USER_ID $GROUP_ID $2

#The script generation process uses the java ProcessParallelDataFiles class.
#Compile the java classes for the client first.
bash $DIR/compileCreateScript.sh

printf "\n%s\n\n" "${mag}Generate a script to move the generated files to subdirectories.${end}"
docker run --rm --user $USER_ID:$GROUP_ID --name clientbuildercontainer \
--volume $DIR/vols/hive:/vols/hive \
--volume $DIR/client/project:/project \
--entrypoint mvn clientbuilder:dev \
-q exec:java -Dexec.mainClass="org.bsc.dcc.vcv.ProcessParallelDataFiles" \
-Dexec.args="/vols/hive/$1GB moveParallelGenFiles.sh" -f /project/pomCreateScript.xml  

printf "\n%s\n\n" "${mag}Moving the generated files to subdirectories using the script.${end}"
bash $DIR/vols/hive/$1GB/moveParallelGenFiles.sh

printf "\n%s\n\n" "${mag}Removing the script.${end}"
rm $DIR/vols/hive/$1GB/moveParallelGenFiles.sh





