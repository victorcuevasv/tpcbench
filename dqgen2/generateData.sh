#!/bin/bash

red=$'\e[1;31m'
grn=$'\e[1;32m'
yel=$'\e[1;33m'
blu=$'\e[1;34m'
mag=$'\e[1;35m'
cyn=$'\e[1;36m'
end=$'\e[0m'

#The scale factor is passed as an argument.
#Also receives as parameters the user and group id of the user who is executing this script.
#$1 scale factor (positive integer)
#$2 user id
#$3 group id

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null && pwd )"

#The globalVars.sh script defines the TPCDS_VERSION variable.
source $DIR/globalVars.sh

#Generate the data.
printf "\n%s\n\n" "${cyn}Generating the data.${end}"

mkdir $DIR/../vols/hive/$1GB

#Get the time of start of execution to measure total execution time.
start_time=$(date +%s)

#Use the 001 (c octal) character to separate columns.
docker run --rm --user $2:$3 --name tpc --volume $DIR/../vols/hive:/TPC-DS/$TPCDS_VERSION/output \
	--entrypoint /TPC-DS/$TPCDS_VERSION/tools/dsdgen tpcds:dev \
	-scale $1 -dir ../output/$1GB  -terminate n -delimiter $(echo -e "\001")

end_time=$(date +%s)

runtime=$((end_time-start_time))
printf "\n%s\n\n" "${cyn}Total execution time: ${runtime} sec.${end}\n"


