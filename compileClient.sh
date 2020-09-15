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

if [ $# -lt 1 ]; then
    echo "${yel}Usage: bash compileClient.sh <use timestamp (1 or 0)>${end}"
    exit 0
fi

Timestamp="$(date +%s)"
UploadedFileName="client-1.2-SNAPSHOT-SHADED.jar"
if [ $1 -eq 1 ]; then
	UploadedFileName="client-1.2-SNAPSHOT-SHADED-${Timestamp}.jar"
fi

#Build the client project.
printf "\n\n%s\n\n" "${blu}Compiling the client project.${end}"
bash client/compile.sh $USER_ID $GROUP_ID

cp client/project/targetjdbc/client-1.2-SNAPSHOT-SHADED.jar \
$HOME/tpcds-jars/targetjdbc/$UploadedFileName

echo "${blu}Uploaded file with name: ${UploadedFileName}.${end}"   

